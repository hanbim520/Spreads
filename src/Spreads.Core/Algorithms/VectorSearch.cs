﻿// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Spreads.DataTypes;
using Spreads.Native;

namespace Spreads.Algorithms
{
    /// <summary> 
    /// Algorithms to find values in contiguous memory region.
    /// WARNING: Methods in this static class do not perform bound checks and are intended to be used
    /// as building blocks in other parts that calculate bounds correctly and
    /// do performs required checks on external input.
    /// </summary>
    public static partial class VectorSearch
    {
        /// <summary>
        /// Optimized binary search that returns the same value as the classic algorithm.
        /// </summary>
        /// <returns>Returns index of the value (if present) or its negative binary complement.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining
#if HAS_AGGR_OPT
                    | MethodImplOptions.AggressiveOptimization
#endif
        )]
        [SuppressMessage("ReSharper", "HeapView.BoxingAllocation")]
        public static int BinarySearch<T>(ref T searchSpace, int length, T value, KeyComparer<T> comparer = default)
        {
            return BinarySearch(ref searchSpace, 0, length, value, comparer);
        }

        /// <summary>
        /// Optimized binary search that returns the same value as the classic algorithm.
        /// </summary>
        /// <returns>Returns index of the value (if present) or its negative binary complement.
        /// The index is relative to <paramref name="searchSpace"/>, not to <paramref name="offset"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining
#if HAS_AGGR_OPT
                    | MethodImplOptions.AggressiveOptimization
#endif
        )]
        [SuppressMessage("ReSharper", "HeapView.BoxingAllocation")]
        public static int BinarySearch<T>(ref T searchSpace, int offset, int length, T value, KeyComparer<T> comparer = default)
        {
#if HAS_INTRINSICS
            if (System.Runtime.Intrinsics.X86.Avx2.IsSupported)
            {
                if (typeof(T) == typeof(sbyte))
                    return BinarySearchAvx2(ref Unsafe.As<T, sbyte>(ref searchSpace), offset, length, Unsafe.As<T, sbyte>(ref value));

                if (typeof(T) == typeof(short))
                    return BinarySearchAvx2(ref Unsafe.As<T, short>(ref searchSpace), offset, length, Unsafe.As<T, short>(ref value));

                if (typeof(T) == typeof(int))
                    return BinarySearchAvx2(ref Unsafe.As<T, int>(ref searchSpace), offset, length, Unsafe.As<T, int>(ref value));

                if (typeof(T) == typeof(long)
                    || typeof(T) == typeof(Timestamp)
                )
                    return BinarySearchAvx2(ref Unsafe.As<T, long>(ref searchSpace), offset, length, Unsafe.As<T, long>(ref value));
            }

            if (System.Runtime.Intrinsics.X86.Sse42.IsSupported)
            {
                if (typeof(T) == typeof(sbyte))
                    return BinarySearchSse42(ref Unsafe.As<T, sbyte>(ref searchSpace), offset, length, Unsafe.As<T, sbyte>(ref value));

                if (typeof(T) == typeof(short))
                    return BinarySearchSse42(ref Unsafe.As<T, short>(ref searchSpace), offset, length, Unsafe.As<T, short>(ref value));

                if (typeof(T) == typeof(int))
                    return BinarySearchSse42(ref Unsafe.As<T, int>(ref searchSpace), offset, length, Unsafe.As<T, int>(ref value));

                if (typeof(T) == typeof(float))
                    return BinarySearchSse42(ref Unsafe.As<T, float>(ref searchSpace), offset, length, Unsafe.As<T, float>(ref value));

                if (typeof(T) == typeof(double))
                    return BinarySearchSse42(ref Unsafe.As<T, double>(ref searchSpace), offset, length, Unsafe.As<T, double>(ref value));

                if (typeof(T) == typeof(long)
                    || typeof(T) == typeof(Timestamp)
                )
                    return BinarySearchSse42(ref Unsafe.As<T, long>(ref searchSpace), offset, length, Unsafe.As<T, long>(ref value));
            }
#endif
            return BinarySearchHybrid(ref searchSpace, offset, length, value, comparer);
        }

        /// <summary>
        /// Performs classic binary search and returns index of the value or its negative binary complement.
        /// Used mostly for correctness check and benchmark baseline for other faster implementations.
        /// Use <see cref="SortedSearch{T}(ref T,int,T,Spreads.KeyComparer{T})"/> for the best performance. 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining
#if HAS_AGGR_OPT
                    | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal static int BinarySearchClassic<T>(ref T searchSpace, int offset, int length, T value, KeyComparer<T> comparer = default)
        {
            unchecked
            {
                int lo = offset;
                int hi = offset + length - 1;
                // If length == 0, hi == -1, and loop will not be entered
                while (lo <= hi)
                {
                    // PERF: `lo` or `hi` will never be negative inside the loop,
                    //       so computing median using uints is safe since we know
                    //       `length <= int.MaxValue`, and indices are >= 0
                    //       and thus cannot overflow an uint.
                    //       Saves one subtraction per loop compared to
                    //       `int i = lo + ((hi - lo) >> 1);`
                    int i = (int) (((uint) hi + (uint) lo) >> 1);

                    int c = comparer.Compare(value, UnsafeEx.ReadUnaligned(ref Unsafe.Add(ref searchSpace, i)));

                    if (c == 0)
                    {
                        return i;
                    }

                    if (c > 0)
                    {
                        lo = i + 1;
                    }
                    else
                    {
                        hi = i - 1;
                    }
                }

                // If none found, then a negative number that is the bitwise complement
                // of the index of the next element that is larger than or, if there is
                // no larger element, the bitwise complement of `length`, which
                // is `lo` at this point.
                return ~lo;
            }
        }

        /// <summary>
        /// Classic binary search that falls back to linear search for the last small number of elements.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining
#if HAS_AGGR_OPT
                    | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal static int BinarySearchHybrid<T>(ref T searchSpace, int offset, int length, T value, KeyComparer<T> comparer = default)
        {
            unchecked
            {
                int c;
                int lo = offset;
                int hi = offset + length - 1;
                while (hi - lo > 7)
                {
                    int i = (int) (((uint) hi + (uint) lo) >> 1);

                    c = comparer.Compare(value, UnsafeEx.ReadUnaligned(ref Unsafe.Add(ref searchSpace, i)));

                    if (c > 0)
                    {
                        lo = i + 1;
                    }
                    else
                    {
                        if (c == 0)
                            goto RETURN;

                        hi = i - 1;
                    }
                }

                while ((c = comparer.Compare(value, UnsafeEx.ReadUnaligned(ref Unsafe.Add(ref searchSpace, lo)))) > 0
                       && ++lo <= hi)
                {
                }

                RETURN:
                var ceq1 = -UnsafeEx.Ceq(c, 0);
                return (ceq1 & lo) | (~ceq1 & ~lo);
            }
        }

        /// <summary>
        /// Converts a result of a sorted search to result of directional search with direction of <see cref="Lookup"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int SearchToLookup<T>(int offset, int length, Lookup lookup, int i, ref T searchSpace, ref T value)
        {
            if (i >= offset)
            {
                if (lookup.IsEqualityOK())
                    goto RETURN;

                if (lookup == Lookup.LT)
                {
                    if (i == offset)
                        goto RETURN_O;

                    i--;
                }
                else // depends on if (eqOk) above
                {
                    Debug.Assert(lookup == Lookup.GT);
                    if (i == offset + length - 1)
                        goto RETURN_OL;

                    i++;
                }
            }
            else
            {
                if (lookup == Lookup.EQ)
                    goto RETURN;

                i = ~i;

                // LT or LE
                if (((uint) lookup & (uint) Lookup.LT) != 0)
                {
                    // i is idx of element that is larger, nothing here for LE/LT
                    if (i == offset)
                        goto RETURN_O;

                    i--;
                }
                else
                {
                    Debug.Assert(((uint) lookup & (uint) Lookup.GT) != 0);
                    Debug.Assert(i <= offset + length);
                    // if was negative, if it was ~length then there are no more elements for GE/GT
                    if (i == offset + length)
                        goto RETURN_OL;
                    // i is the same, ~i is idx of element that is GT the value
                }
            }

            value = UnsafeEx.ReadUnaligned(ref Unsafe.Add(ref searchSpace, i));
            
            RETURN:
            Debug.Assert(unchecked((uint) i) - offset < unchecked((uint) length));
            return i;

            RETURN_O:
            return ~offset;

            RETURN_OL:
            return ~(offset + length);
        }

        /// <summary>
        /// Find value using binary search according to the lookup direction.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int BinaryLookup<T>(ref T searchSpace, int length, ref T value, Lookup lookup, KeyComparer<T> comparer = default)
        {
            return BinaryLookup(ref searchSpace, 0, length, ref value, lookup, comparer);
        }

        /// <summary>
        /// Find value using binary search according to the lookup direction.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int BinaryLookup<T>(ref T searchSpace, int offset, int length, ref T value, Lookup lookup, KeyComparer<T> comparer = default)
        {
            Debug.Assert(length >= 0);
            var i = BinarySearch(ref searchSpace, offset, length, value, comparer);
            return SearchToLookup<T>(offset, length, lookup, i, ref searchSpace, ref value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int InterpolationSearch<T>(ref T searchSpace, int length, T value, KeyComparer<T> comparer = default)
        {
            return InterpolationSearch(ref searchSpace, offset: 0, length, value, comparer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int InterpolationSearch<T>(ref T searchSpace, int offset, int length, T value, KeyComparer<T> comparer = default)
        {
            if (typeof(T) == typeof(long)
                || typeof(T) == typeof(Timestamp))
                return InterpolationSearchSpecialized(ref Unsafe.As<T, long>(ref searchSpace), offset, length, Unsafe.As<T, long>(ref value));

            if (typeof(T) == typeof(ulong))
                return InterpolationSearchSpecialized(ref Unsafe.As<T, ulong>(ref searchSpace), offset, length, Unsafe.As<T, ulong>(ref value));

            if (typeof(T) == typeof(int))
                return InterpolationSearchSpecialized(ref Unsafe.As<T, int>(ref searchSpace), offset, length, Unsafe.As<T, int>(ref value));

            if (typeof(T) == typeof(uint))
                return InterpolationSearchSpecialized(ref Unsafe.As<T, uint>(ref searchSpace), offset, length, Unsafe.As<T, uint>(ref value));

            if (typeof(T) == typeof(short))
                return InterpolationSearchSpecialized(ref Unsafe.As<T, short>(ref searchSpace), offset, length, Unsafe.As<T, short>(ref value));

            if (typeof(T) == typeof(ushort))
                return InterpolationSearchSpecialized(ref Unsafe.As<T, ushort>(ref searchSpace), offset, length, Unsafe.As<T, ushort>(ref value));

            if (typeof(T) == typeof(char))
                return InterpolationSearchSpecialized(ref Unsafe.As<T, char>(ref searchSpace), offset, length, Unsafe.As<T, char>(ref value));

            if (typeof(T) == typeof(byte))
                return InterpolationSearchSpecialized(ref Unsafe.As<T, byte>(ref searchSpace), offset, length, Unsafe.As<T, byte>(ref value));

            if (typeof(T) == typeof(sbyte))
                return InterpolationSearchSpecialized(ref Unsafe.As<T, sbyte>(ref searchSpace), offset, length, Unsafe.As<T, sbyte>(ref value));

            if (typeof(T) == typeof(float))
                return InterpolationSearchSpecialized(ref Unsafe.As<T, float>(ref searchSpace), offset, length, Unsafe.As<T, float>(ref value));

            if (typeof(T) == typeof(double))
                return InterpolationSearchSpecialized(ref Unsafe.As<T, double>(ref searchSpace), offset, length, Unsafe.As<T, double>(ref value));

            if (!KeyComparer<T>.Default.IsDiffable)
                return BinarySearch(ref searchSpace, offset, length, value, comparer);

            return InterpolationSearchGeneric(ref searchSpace, offset, length, value, comparer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int InterpolationSearchGeneric<T>(ref T searchSpace, int offset, int length, T value, KeyComparer<T> comparer = default)
        {
            // Try interpolation only for big-enough lengths and do minimal job,
            // just find the range with exponential search with minimal branches
            // and switch to binary search.
            unchecked
            {
                int i;
                int lo = offset;
                int hi = offset + length - 1;

                if (hi - lo > 16)
                {
                    var vlo = UnsafeEx.ReadUnaligned(ref searchSpace);
                    var vhi = UnsafeEx.ReadUnaligned(ref Unsafe.Add(ref searchSpace, hi));
                    int range = hi - lo;
                    long vRange = comparer.Diff(vhi, vlo);

                    // (hi - lo) <= int32.MaxValue
                    // vlo could be zero while value could easily be close to int64.MaxValue (nanos in unix time, we are now between 60 and 61 bit at 60.4)
                    // convert to double here to avoid overflow and for much faster calculations
                    // (only 4 cycles vs 25 cycles https://lemire.me/blog/2017/11/16/fast-exact-integer-divisions-using-floating-point-operations/)
                    double nominator = (hi - lo) * (double) comparer.Diff(value, vlo);

                    i = (int) (nominator / vRange);

                    if ((uint) i > range)
                        i = i < 0 ? 0 : range;
                    // make i relative to vecStart
                    i += lo;

                    int c = comparer.Compare(value, UnsafeEx.ReadUnaligned(ref Unsafe.Add(ref searchSpace, i)));

                    if (c == 0)
                        goto FOUND;

                    var step = 1;

                    if (c > 0)
                    {
                        while (true)
                        {
                            i += step;

                            if (i > hi)
                                break;

                            c = comparer.Compare(value, UnsafeEx.ReadUnaligned(ref Unsafe.Add(ref searchSpace, i)));

                            if (c <= 0)
                            {
                                if (c == 0)
                                    goto FOUND;

                                hi = i - 1;
                                break;
                            }

                            step <<= 1;
                        }

                        lo = i - step + 1;
                    }
                    else
                    {
                        while (true)
                        {
                            i -= step;

                            if (i < lo)
                                break;

                            c = comparer.Compare(value, UnsafeEx.ReadUnaligned(ref Unsafe.Add(ref searchSpace, i)));

                            if (c >= 0)
                            {
                                if (c == 0)
                                    goto FOUND;

                                lo = i + 1;
                                break;
                            }

                            step <<= 1;
                        }

                        hi = i + step - 1;
                    }
                }

                return BinarySearch(ref searchSpace, lo, 1 + hi - lo, value);

                FOUND:
                return i;
            }
        }

        /// <summary>
        /// Find value using binary search according to the lookup direction.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int InterpolationLookup<T>(ref T searchSpace, int length, ref T value, Lookup lookup,
            KeyComparer<T> comparer = default)
        {
            return InterpolationLookup(ref searchSpace, offset: 0, length, ref value, lookup, comparer);
        }

        /// <summary>
        /// Find value using binary search according to the lookup direction.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int InterpolationLookup<T>(ref T searchSpace, int offset, int length, ref T value, Lookup lookup,
            KeyComparer<T> comparer = default)
        {
            Debug.Assert(length >= 0);

            var i = InterpolationSearch(ref searchSpace, offset, length, value, comparer);
            return SearchToLookup(offset, length, lookup, i, ref searchSpace, ref value);
        }

        /// <summary>
        /// Performs interpolation search for well-known types and binary search for other types.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int SortedSearch<T>(ref T searchSpace, int length, T value, KeyComparer<T> comparer = default)
        {
            return SortedSearch(ref searchSpace, offset: 0, length, value, comparer);
        }

        /// <summary>
        /// Performs interpolation search for well-known types and binary search for other types.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int SortedSearch<T>(ref T searchSpace, int offset, int length, T value, KeyComparer<T> comparer = default)
        {
            // ReSharper disable once RedundantLogicalConditionalExpressionOperand
            if (Settings.UseInterpolatedSearchForKnownTypes && KeyComparer<T>.IsDiffableSafe)
                return InterpolationSearch(ref searchSpace, offset, length, value, comparer);

            return BinarySearch(ref searchSpace, offset, length, value, comparer);
        }

        /// <summary>
        /// Performs interpolation lookup for well-known types and binary lookup for other types.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int SortedLookup<T>(ref T searchSpace, int length, ref T value, Lookup lookup, KeyComparer<T> comparer = default)
        {
            return SortedLookup(ref searchSpace, 0, length, ref value, lookup, comparer);
        }

        /// <summary>
        /// Performs interpolation lookup for well-known types and binary lookup for other types.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int SortedLookup<T>(ref T searchSpace, int offset, int length, ref T value, Lookup lookup, KeyComparer<T> comparer = default)
        {
            // ReSharper disable once RedundantLogicalConditionalExpressionOperand
            if (Settings.UseInterpolatedSearchForKnownTypes && KeyComparer<T>.IsDiffableSafe)
                return InterpolationLookup(ref searchSpace, offset, length, ref value, lookup, comparer);

            return BinaryLookup(ref searchSpace, offset, length, ref value, lookup, comparer);
        }
    }
}