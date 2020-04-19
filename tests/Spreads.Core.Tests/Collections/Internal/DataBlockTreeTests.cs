// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using System;
using NUnit.Framework;
using Spreads.Collections.Internal;
using Spreads.Native;
using Spreads.Utils;

#pragma warning disable 618

namespace Spreads.Core.Tests.Collections.Internal
{
    [Category("CI")]
    [TestFixture]
    public class DataBlockTreeTests
    {
        [Test]
        public void CouldAppend()
        {
            var blockLimit = Settings.MIN_POOLED_BUFFER_LEN * 2;
            DataBlock.MaxLeafSize = blockLimit;
            DataBlock.MaxNodeSize = blockLimit;

            var db = DataBlock.CreateSeries<int, int>();
            var lastBlock = db;
            for (int i = 0; i < blockLimit; i++)
            {
                DataBlock.Append<int, int>(db, ref lastBlock, i, i);
            }

            db.ReferenceCount.ShouldEqual(0);
            db.RowCapacity.ShouldEqual(blockLimit);
            db.RowCount.ShouldEqual(blockLimit);

            // First height increase
            DataBlock.Append<int, int>(db, ref lastBlock, blockLimit, blockLimit);

            db.Height.ShouldEqual(1);
            db.RowCount.ShouldEqual(2);
            db.RowCapacity.ShouldEqual(Settings.MIN_POOLED_BUFFER_LEN);
            db.Values.RuntimeTypeId.ShouldEqual(VecTypeHelper<DataBlock>.RuntimeTypeId);
            db.Values.UnsafeReadUnaligned<DataBlock>(0).ShouldNotEqual(db);

            db.Values.UnsafeReadUnaligned<DataBlock>(0).NextBlock.ShouldBeSame(db.Values.UnsafeReadUnaligned<DataBlock>(1));
            db.Values.UnsafeReadUnaligned<DataBlock>(1).PreviousBlock.ShouldBeSame(db.Values.UnsafeReadUnaligned<DataBlock>(0));

            var value = blockLimit + 1;
            // root has one block and could have up to blockLimit blocks
            for (int h1 = 1; h1 < blockLimit; h1++)
            {
                for (int i = 0; i < blockLimit; i++)
                {
                    if (h1 == 1 && i == 0)
                        continue;
                    DataBlock.Append<int, int>(db, ref lastBlock, value, value);
                    value++;
                }
            }

            db.Height.ShouldEqual(1);
            db.RowCount.ShouldEqual(blockLimit);
            db.RowCapacity.ShouldEqual(blockLimit);

            for (int i = 1; i < blockLimit - 1; i++)
            {
                var blockI = db.Values.UnsafeReadUnaligned<DataBlock>(i);
                var blockIPrev = db.Values.UnsafeReadUnaligned<DataBlock>(i - 1);
                var blockINext = db.Values.UnsafeReadUnaligned<DataBlock>(i + 1);

                blockIPrev.NextBlock.ShouldBeSame(blockI);
                blockI.PreviousBlock.ShouldBeSame(blockIPrev);

                blockI.NextBlock.ShouldBeSame(blockINext);
                blockINext.PreviousBlock.ShouldBeSame(blockI);
            }

            var firstBlock = db.Values.UnsafeReadUnaligned<DataBlock>(0);
            firstBlock.PreviousBlock.ShouldBeNull();
            var last = db.Values.UnsafeReadUnaligned<DataBlock>(blockLimit - 1);
            last.NextBlock.ShouldBeNull();

            // Second height increase
            DataBlock.Append<int, int>(db, ref lastBlock, value, value);
            value++;

            db.Height.ShouldEqual(2);
            db.RowCount.ShouldEqual(2);
            db.RowCapacity.ShouldEqual(Settings.MIN_POOLED_BUFFER_LEN);

            for (int h2 = 1; h2 < blockLimit; h2++)
            {
                for (int h1 = 0; h1 < blockLimit; h1++)
                {
                    for (int i = 0; i < blockLimit; i++)
                    {
                        if (h2 == 1 && h1 == 0 && i == 0)
                            continue;
                        DataBlock.Append<int, int>(db, ref lastBlock, value, value);
                        value++;
                    }
                }
            }

            db.Height.ShouldEqual(2);

            firstBlock = db.Values.UnsafeReadUnaligned<DataBlock>(0).Values.UnsafeReadUnaligned<DataBlock>(0);
            var block = firstBlock;
            var count = 0;
            while (true)
            {
                if (block.NextBlock == null)
                    break;

                block.NextBlock.PreviousBlock.ShouldEqual(block);
                block = block.NextBlock;
                count++;
            }

            count.ShouldEqual(blockLimit * blockLimit - 1);
            last = block;
            count = 0;
            while (true)
            {
                if (block.PreviousBlock == null)
                    break;

                block.PreviousBlock.NextBlock.ShouldEqual(block);
                block = block.PreviousBlock;
                count++;
            }

            count.ShouldEqual(blockLimit * blockLimit - 1);

            for (int i = 0; i < blockLimit; i++)
            {
                var blockI = db.Values.UnsafeReadUnaligned<DataBlock>(i);

                blockI.PreviousBlock.ShouldBeNull();
                blockI.NextBlock.ShouldBeNull();
            }

            // Third height increase
            DataBlock.Append<int, int>(db, ref lastBlock, value, value);
            value++;

            db.Height.ShouldEqual(3);

            db.Dispose();

            GC.Collect(2, GCCollectionMode.Forced, true, true);
            GC.WaitForPendingFinalizers();
            GC.Collect(2, GCCollectionMode.Forced, true, true);
            GC.WaitForPendingFinalizers();
        }

        [Test
#if RELEASE
         , Explicit("Benchmark")
#endif
        ]
        public unsafe void CouldAppendBench()
        {
            // var blockLimit = Settings.MIN_POOLED_BUFFER_LEN;
            // DataBlock.MaxLeafSize = blockLimit;
            // DataBlock.MaxNodeSize = blockLimit;
            var count = TestUtils.GetBenchCount(100_000_000, 1000);
            var rounds = 2;

            for (int r = 0; r < rounds; r++)
            {
                var db = DataBlock.CreateSeries<int, int>();
                var lastBlock = db;
                using (Benchmark.Run("Append", count))
                {
                    for (int i = 0; i < count; i++)
                    {
                        DataBlock.Append<int, int>(db, ref lastBlock, i, i);
                    }
                }

                using (Benchmark.Run("SearchKey", count))
                {
                    for (int i = 1; i < count; i++)
                    {
                        var key = i;
                        var idx = 0;
                        if ((idx = DataBlock.SearchKey(db, key, KeyComparer<int>.Default, out var b)) < 0)
                        {
                            Assert.Fail($"Cannot find existing key {i} - {key} - {idx}");
                        }
                    }
                }

                Console.WriteLine($"Height: {db.Height}");
                db.Dispose();
            }

            Benchmark.Dump();
        }

        [Test, Explicit("Benchmark")]
        public void CouldAppendIntMax()
        {
            var count = (long) Int32.MaxValue;

            for (int r = 0; r < 2; r++)
            {
                var db = DataBlock.CreateSeries<int, int>();
                var lastBlock = db;
                using (Benchmark.Run("DB.Tree.Append", count))
                {
                    for (long i = 0; i < count; i++)
                    {
                        DataBlock.Append<int, int>(db, ref lastBlock, (int) i, (int) i);
                    }
                }

                using (Benchmark.Run("SearchKey", count))
                {
                    for (int i = 1; i < count; i++)
                    {
                        if (DataBlock.SearchKey(db, i, KeyComparer<int>.Default, out var b) < 0)
                        {
                            Assert.Fail($"Cannot find existing key {i}");
                        }
                    }
                }
                
                Console.WriteLine($"Height: {db.Height}");
                db.Dispose();
            }

            Benchmark.Dump();
        }
    }
}