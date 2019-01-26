﻿// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using Spreads.Algorithms;
using Spreads.Collections.Internal;
using Spreads.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Spreads.Collections
{
    /// <summary>
    /// Base class for data containers implementations.
    /// </summary>
    [CannotApplyEqualityOperator]
    public class BaseSeries // TODO rename to BaseContainer
    {
        internal BaseSeries()
        { }

        internal DataBlockStorage DataBlock;

        #region Attributes

        private static readonly ConditionalWeakTable<BaseSeries, Dictionary<string, object>> Attributes =
            new ConditionalWeakTable<BaseSeries, Dictionary<string, object>>();

        /// <summary>
        /// Get an attribute that was set using SetAttribute() method.
        /// </summary>
        /// <param name="attributeName">Name of an attribute.</param>
        /// <returns>Return an attribute value or null is the attribute is not found.</returns>
        public object GetAttribute(string attributeName)
        {
            if (Attributes.TryGetValue(this, out Dictionary<string, object> dic) &&
                dic.TryGetValue(attributeName, out object res))
            {
                return res;
            }
            return null;
        }

        /// <summary>
        /// Set any custom attribute to a series. An attribute is available during lifetime of a series and is available via GetAttribute() method.
        /// </summary>
        public void SetAttribute(string attributeName, object attributeValue)
        {
            var dic = Attributes.GetOrCreateValue(this);
            dic[attributeName] = attributeValue;
        }

        #endregion Attributes
    }

    /// <inheritdoc />
    public class BaseContainer<TKey> : BaseSeries
    {
        // for tests only, it should have been abstract otherwise
        internal BaseContainer()
        {
        }

        internal KeyComparer<TKey> Comparer = default;
        internal DataBlockSource<TKey> DataSource;

        // TODO we are forking existing series implementation from here
        // All containers inherit this.
        // Matrix has Int64 key.
        // Matrix could be sparse, it is no more than a series of rows.
        // All series functionality should be moved to new Series

        internal bool IsSingleBlock
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => DataSource == null;
        }

        /// <summary>
        /// Returns <see cref="DataBlockStorage"/> that contains <paramref name="index"></paramref> and local index within the block as <paramref name="blockIndex"></paramref>.
        /// </summary>
        /// <param name="index">Index to get element at.</param>
        /// <param name="block"><see cref="DataBlockStorage"/> that contains <paramref name="index"></paramref> or null if not found.</param>
        /// <param name="blockIndex">Local index within the block. -1 if requested index is not range.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryGetBlockAt(long index, out DataBlockStorage block, out int blockIndex)
        {
            // Take reference, do not work directly. Reference assignment is atomic in .NET
            block = null;
            blockIndex = -1;
            var result = false;

            if (IsSingleBlock)
            {
                Debug.Assert(DataBlock != null, "Single-block series must always have non-null DataBlock");

                if (index < DataBlock.RowLength)
                {
                    block = DataBlock;
                    blockIndex = (int)index;
                    result = true;
                }
            }
            else
            {
                // TODO check DataBlock range, probably we are looking in the same block
                // But for this search it is possible only for immutable or append-only
                // because we need to track first global index. For such cases maybe
                // we should just guarantee that DataSource.ConstantBlockLength > 0 and is pow2.

                var constantBlockLength = DataSource.ConstantBlockLength;
                if (constantBlockLength > 0)
                {
                    // TODO review long division. constantBlockLength should be a poser of 2
                    var sourceIndex = index / constantBlockLength;
                    if (DataSource.TryGetAt(sourceIndex, out var kvp))
                    {
                        block = kvp.Value;
                        blockIndex = (int)(index - sourceIndex * constantBlockLength);
                        result = true;
                    }
                }
                else
                {
                    result = TryGetBlockAtSlow(index, out block, out blockIndex);
                }
            }

            return result;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        // ReSharper disable once UnusedParameter.Local
        private static bool TryGetBlockAtSlow(long index, out DataBlockStorage block, out int blockIndex)
        {
            // TODO slow path as non-inlined method
            throw new NotImplementedException();
            //foreach (var kvp in DataSource)
            //{
            //    // if (kvp.Value.RowLength)
            //}
        }

        /// <summary>
        /// When found, updates key by the found key if it is different, returns block and index whithin the block where the data resides.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="lookup"></param>
        /// <param name="block"></param>
        /// <param name="blockIndex"></param>
        /// <param name="updateDataBlock"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryFindBlockAt(ref TKey key, Lookup lookup, out DataBlockStorage block, out int blockIndex, 
            bool updateDataBlock = false)
        {
            // This is non-obvious part:
            // For most searches we could find the right block
            // by searching with LE on the source:
            // o - EQ match, x - non-existing target key
            // | - start of a block, there is no space between start of block and it's first element (TODO review, had issues with that)
            // * for LE
            //      - [...] |[o...] |[<..]
            //      - [...] |[.o..] |[<..]
            //      - [...] |[ox..] |[<..]
            //      - [...] |[...o]x|[<..]
            // * for EQ
            //      - [...] |[x...] |[<..] - not possible, because with LE this would be [..o]x|[....] | [<..] since the first key must be LE, if it is not !EQ we find previous block
            //      - [...] |[o...] |[<..]
            //      - [...] |[..o.] |[<..]
            //      - [...] |[..x.] |[<..]
            //      - [...] |[....]x|[<..]
            // * for GE
            //      - [...] |[o...] |[<..]
            //      - [...] |[xo..] |[<..]
            //      - [...] |[..o.] |[<..]
            //      - [...] |[..xo] |[<..]
            //      - [...] |[...x] |[o..] SPECIAL CASE, SLE+SS do not find key but it is in the next block if it exist
            //      - [...] |[....]x|[o..] SPECIAL CASE
            // * for GT
            //      - [...] |[xo..] |[<..]
            //      - [...] |[.xo.] |[<..]
            //      - [...] |[...x] |[o..] SPECIAL CASE
            //      - [...] |[..xo] |[<..]
            //      - [...] |[...x] |[o..] SPECIAL CASE
            //      - [...] |[....]x|[o..] SPECIAL CASE

            // for LT we need to search by LT

            // * for LT
            //      - [..o] |[x...] |[<..]
            //      - [...] |[ox..] |[<..]
            //      - [...] |[...o]x|[<..]

            // So the algorithm is:
            // Search source by LE or by LE if lookup is LT
            // Do SortedSearch on the block
            // If not found check if complement is after the end and

            // Notes: always optimize for LE search, it should have least branches and could try speculatively
            // even if we could detect special cases in advance. Only when we cannot find check if there was a
            // special case and process it in a slow path as non-inlined method.

            block = DataBlock;
            bool retryOnGt = default;

            if (DataSource != null)
            {
                retryOnGt = true;
                TryFindBlock_ValidateOrGetBlockFromSource(ref block, key, lookup, lookup == Lookup.LT ? Lookup.LT : Lookup.LE);


                // TODO (review) updating cache is not responsibility of this method
                // There could be a situation when we know that a search is irregular
                // Also we return the block from this method so a caller could update itself.
                // Cursors should not update

                // Even if we do not find the key update cache anyway here unconditionally to search result below,
                // do not penalize single-block case with this op (significant)
                // and likely the next search will be around current value anyway
                if (updateDataBlock)
                {
                    DataBlock = block;
                }

                
            }

        RETRY:

            Debug.Assert(block != null);

            // Here we use internal knowledge that for series RowIndex in contiguous vec
            // TODO(?) do check if VS is pure, allow strides > 1 or just create Nth cursor?

            Debug.Assert(block.RowIndex._stride == 1);

            // TODO optimize this search via non-generic IVector with generic getter
            // ReSharper disable once PossibleNullReferenceException
            blockIndex = VectorSearch.SortedLookup(ref block.RowIndex.DangerousGetRef<TKey>(0), 
                block.RowLength, ref key, lookup, Comparer);

            if (blockIndex >= 0)
            {
                if (updateDataBlock)
                {
                    DataBlock = block;
                }
                return true;
            }

            // Check for SPECIAL CASE from the comment above
            if (retryOnGt &&
                (~blockIndex) == block.RowLength
                && ((int)lookup & (int)Lookup.GT) != 0)
            {
                retryOnGt = false;
                var nextBlock = block.TryGetNextBlock();
                if (nextBlock == null)
                {
                    TryFindBlock_ValidateOrGetBlockFromSource(ref nextBlock,
                        block.RowIndex.DangerousGetRef<TKey>(0), lookup, Lookup.GT);
                }

                if (nextBlock != null)
                {
                    block = nextBlock;
                    goto RETRY;
                }
            }

            return false;
        }

        // TODO Test multi-block case and this attribute impact. Maybe direct call is OK without inlining
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void TryFindBlock_ValidateOrGetBlockFromSource(ref DataBlockStorage block,
            TKey key, Lookup direction, Lookup sourceDirection)
        {
            // for single block this should exist, for sourced blocks this value is updated by a last search
            // Take reference, do not work directly. Reference assignment is atomic in .NET

            if (block != null) // cached block
            {
                // Check edge cases if key is outside the block and we may need to retrieve
                // the right one from storage. We do not know anything about other blocks, so we must
                // be strictly in range so that all searches will work.

                if (block.RowLength <= 1) // with 1 there are some edge cases that penalize normal path, so make just one comparison
                {
                    block = null;
                }
                else
                {
                    var firstC = Comparer.Compare(key, block.RowIndex.DangerousGet<TKey>(0));

                    if (firstC < 0 // not in this block even if looking LT
                        || direction == Lookup.LT // first value is >= key so LT won't find the value in this block
                                                  // Because rowLength >= 2 we do not need to check for firstC == 0 && GT
                    )
                    {
                        block = null;
                    }
                    else
                    {
                        var lastC = Comparer.Compare(key, block.RowIndex.DangerousGet<TKey>(block.RowLength - 1));

                        if (lastC > 0
                            || direction == Lookup.GT
                        )
                        {
                            block = null;
                        }
                    }
                }
            }
            // if block is null here we have rejected it and need to get it from source
            // or it is the first search and cached block was not set yet
            if (block == null)
            {
                // Lookup sourceDirection = direction == Lookup.LT ? Lookup.LT : Lookup.LE;
                // TODO review: next line will eventually call this method for in-memory case, so how inlining possible?
                // compiler should do magic to convert all this to a loop at JIT stage, so likely it does not
                // and the question is where to break the chain. We probably could afford non-inlined
                // DataSource.TryFindAt if this method will be faster for single-block and cache-hit cases.
                if (!DataSource.TryFindAt(key, sourceDirection, out var kvp))
                {
                    block = null;
                }
                else
                {
                    if (AdditionalCorrectnessChecks.Enabled)
                    {
                        if (kvp.Value.RowLength <= 0 || Comparer.Compare(kvp.Key, kvp.Value.RowIndex.DangerousGet<TKey>(0)) != 0)
                        {
                            ThrowBadBlockFromSource();
                        }
                    }

                    block = kvp.Value;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private DataBlockStorage TryFindBlockAt_LookUpSource(TKey sourceKey, Lookup direction)
        {
            // TODO review: next line will eventually call this method for in-memory case, so how inlining possible?
            // compiler should do magic to convert all this to a loop at JIT stage, so likely it does not
            // and the question is where to break the chain. We probably could afford non-inlined
            // DataSource.TryFindAt if this method will be faster for single-block and cache-hit cases.
            if (!DataSource.TryFindAt(sourceKey, direction, out var kvp))
            {
                return null;
            }

            if (AdditionalCorrectnessChecks.Enabled)
            {
                if (kvp.Value.RowLength <= 0 || Comparer.Compare(kvp.Key, kvp.Value.RowIndex.DangerousGet<TKey>(0)) != 0)
                {
                    ThrowBadBlockFromSource();
                }
            }

            return kvp.Value;
        }

        private static void ThrowBadBlockFromSource()
        {
            ThrowHelper.ThrowInvalidOperationException("BaseContainer.DataSource.TryFindAt " +
                    "returned an empty block or key that doesn't match the first row index value");
        }
    }
}