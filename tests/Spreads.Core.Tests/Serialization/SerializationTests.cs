﻿// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using NUnit.Framework;
using Spreads.Blosc;
using Spreads.Collections;
using Spreads.DataTypes;
using Spreads.Serialization;
using Spreads.Serialization.Utf8Json;
using Spreads.Utils;
using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace Spreads.Core.Tests.Serialization
{
    [TestFixture]
    public class SerializationTests
    {
        [Serialization(BlittableSize = 12)]
        [StructLayout(LayoutKind.Sequential, Pack = 4)]
        public struct BlittableStruct
        {
            public int Value1;
            public long Value2;
        }

        public class SimplePoco : IEquatable<SimplePoco>
        {
            public int Value1;
            public string Value2;

            public bool Equals(SimplePoco other)
            {
                return this.Value1 == other.Value1 && this.Value2 == other.Value2;
            }
        }

        //[Test, Throws(typeof(System.ArgumentException))]
        //public void CouldNotPinDateTimeArray()
        //{
        //    var dta = new DateTime[2];
        //    GCHandle.Alloc(dta, GCHandleType.Pinned);
        //}

        [Test]
        public void CouldPinDecimalArray()
        {
            var dta = new decimal[2];
            var handle = GCHandle.Alloc(dta, GCHandleType.Pinned);
            handle.Free();
        }

        [Test]
        public void CouldSerializeDateTimeArray()
        {
            var bytes = new byte[1000];
            var dta = new DateTime[2];
            dta[0] = DateTime.Today;
            dta[1] = DateTime.Today.AddDays(1);
            var len = BinarySerializer.Write(dta, ref bytes);
            Assert.AreEqual(8 + 8 * 2, len);
            DateTime[] dta2 = null;
            var len2 = BinarySerializer.Read(bytes, out dta2);
            Assert.AreEqual(len, len2);
            Assert.IsTrue(dta.SequenceEqual(dta2));
        }

        [Test]
        public void CouldSerializeIntArray()
        {
            var bytes = new byte[1000];
            var ints = new int[2];
            ints[0] = 123;
            ints[1] = 456;
            var len = BinarySerializer.Write(ints, ref bytes);
            Assert.AreEqual(8 + 4 * 2, len);
            int[] ints2 = null;
            var len2 = BinarySerializer.Read(bytes, out ints2);
            Assert.AreEqual(len, len2);
            Assert.IsTrue(ints.SequenceEqual(ints2));
        }

        [Test]
        public void CouldSerializeDecimalArray()
        {
            Assert.AreEqual(16, TypeHelper<decimal>.FixedSize);
            var bytes = new byte[1000];
            var decimals = new decimal[2];
            decimals[0] = 123;
            decimals[1] = 456;
            var len = BinarySerializer.Write(decimals, ref bytes);
            Assert.AreEqual(8 + 16 * 2, len);
            decimal[] decimals2 = null;
            var len2 = BinarySerializer.Read(bytes, out decimals2);
            Assert.IsTrue(decimals.SequenceEqual(decimals2));
            Assert.AreEqual(len, len2);
        }

        [Test]
        public void CouldSerializeStringArray()
        {
            var bytes = new byte[1000];
            var arr = new string[2];
            arr[0] = "123";
            arr[1] = "456";
            var len = BinarySerializer.Write(arr, ref bytes);

            string[] arr2 = null;
            var len2 = BinarySerializer.Read(bytes, out arr2);
            Assert.AreEqual(len, len2);
            Assert.IsTrue(arr.SequenceEqual(arr2));
        }

        [Test]
        public void CouldSerializeBlittableStructArray()
        {
            var bytes = new byte[1000];
            var arr = new BlittableStruct[2];
            arr[0] = new BlittableStruct
            {
                Value1 = 123,
                Value2 = 1230
            };
            arr[1] = new BlittableStruct
            {
                Value1 = 456,
                Value2 = 4560
            };
            var len = BinarySerializer.Write(arr, ref bytes);
            Assert.AreEqual(8 + 12 * 2, len);
            BlittableStruct[] arr2 = null;
            var len2 = BinarySerializer.Read(bytes, out arr2);
            Assert.AreEqual(len, len2);
            Assert.IsTrue(arr.SequenceEqual(arr2));
        }

        [Test]
        public void CouldSerializePocoArray()
        {
            var bytes = new byte[1000];
            var arr = new SimplePoco[2];
            arr[0] = new SimplePoco
            {
                Value1 = 123,
                Value2 = "1230"
            };
            arr[1] = new SimplePoco
            {
                Value1 = 456,
                Value2 = "4560"
            };
            var len = BinarySerializer.Write(arr, ref bytes);
            SimplePoco[] arr2 = null;
            var len2 = BinarySerializer.Read(bytes, out arr2);
            Assert.IsTrue(arr.SequenceEqual(arr2), "Items are not equal");
            Assert.AreEqual(len, len2);
        }

        [Test]
        public void CouldSerializeString()
        {
            var bytes = new byte[1000];
            var str = "This is string";
            var len = BinarySerializer.Write(str, ref bytes, timestamp: TimeService.Default.CurrentTime);
            string str2 = null;
            var len2 = BinarySerializer.Read(bytes, out str2);
            Assert.AreEqual(len, len2);
            Assert.AreEqual(str, str2);
        }

        [Test]
        public void JsonWorksWithArraySegment()
        {
            var ints = new int[4] { 1, 2, 3, 4 };
            var segment = new ArraySegment<int>(ints, 1, 2);
            var serialized = Newtonsoft.Json.JsonConvert.SerializeObject(segment);
            Console.WriteLine(serialized);
            var newInts = Newtonsoft.Json.JsonConvert.DeserializeObject<int[]>(serialized);
            Assert.AreEqual(2, newInts[0]);
            Assert.AreEqual(3, newInts[1]);

            var bsonBytes = JsonSerializer.Serialize(segment);
            var newInts2 = JsonSerializer.Deserialize<int[]>(bsonBytes);

            Assert.AreEqual(2, newInts2[0]);
            Assert.AreEqual(3, newInts2[1]);
        }

        [Test]
        public unsafe void CouldSerializeSortedMap()
        {
            SortedMap<DateTime, decimal>.Init();
            var rng = new Random();

            var dest = (Memory<byte>)new byte[1000000];
            var buffer = dest;
            var handle = buffer.Pin();
            var ptr = (IntPtr)handle.Pointer;

            var sm = new SortedMap<DateTime, decimal>();
            for (var i = 0; i < 10000; i++)
            {
                if (i != 2)
                {
                    sm.Add(DateTime.Today.AddHours(i), (decimal)Math.Round(i + rng.NextDouble(), 2));
                }
            }
            var len = BinarySerializer.Write(sm, ref buffer, format: SerializationFormat.BinaryZstd);
            Console.WriteLine($"Useful: {sm.Count * 24.0}");
            Console.WriteLine($"Total: {len}");
            // NB interesting that with converting double to decimal savings go from 65% to 85%,
            // even calculated from (8+8) base size not decimal's 16 size
            Console.WriteLine($"Savings: {1.0 - ((len * 1.0) / (sm.Count * 24.0))}");
            SortedMap<DateTime, decimal> sm2 = null;
            var len2 = BinarySerializer.Read(buffer, out sm2);

            Assert.AreEqual(len, len2);

            Assert.IsTrue(sm2.Keys.SequenceEqual(sm.Keys));
            Assert.IsTrue(sm2.Values.SequenceEqual(sm.Values));
        }

        [Test]
        public unsafe void CouldSerializeRegularSortedMapWithZstd()
        {
            BloscSettings.SerializationFormat = SerializationFormat.BinaryZstd;
            var rng = new Random();

            var dest = (Memory<byte>)new byte[1000000];
            var buffer = dest;
            var handle = buffer.Pin();
            var ptr = (IntPtr)handle.Pointer;

            var sm = new SortedMap<DateTime, decimal>();
            for (var i = 0; i < 1000; i++)
            {
                sm.Add(DateTime.Today.AddSeconds(i), (decimal)Math.Round(i + rng.NextDouble(), 2));
            }

            MemoryStream tmp;
            var size = BinarySerializer.SizeOf(sm, out tmp);
            var len = BinarySerializer.Write(sm, ref dest, tmp);
            Console.WriteLine($"Useful: {sm.Count * 24}");
            Console.WriteLine($"Total: {len}");
            // NB interesting that with converting double to decimal savings go from 65% to 85%,
            // even calculated from (8+8) base size not decimal's 16 size
            Console.WriteLine($"Savings: {1.0 - ((len * 1.0) / (sm.Count * 24.0))}");
            SortedMap<DateTime, decimal> sm2 = null;
            var len2 = BinarySerializer.Read(buffer, out sm2);

            Assert.AreEqual(len, len2);

            Assert.IsTrue(sm2.Keys.SequenceEqual(sm.Keys));
            Assert.IsTrue(sm2.Values.SequenceEqual(sm.Values));
        }

        [Test]
        public unsafe void CouldSerializeSortedMap2()
        {
            var rng = new Random();

            var dest = (Memory<byte>)new byte[1000000];
            var buffer = dest;
            var handle = buffer.Pin();
            var ptr = (IntPtr)handle.Pointer;

            var sm = new SortedMap<int, int>();
            for (var i = 0; i < 10000; i++)
            {
                sm.Add(i, i);
            }
            MemoryStream temp;
            var len = BinarySerializer.SizeOf(sm, out temp);
            var len2 = BinarySerializer.Write(sm, ref buffer, temp);
            Assert.AreEqual(len, len2);
            Console.WriteLine($"Useful: {sm.Count * 8}");
            Console.WriteLine($"Total: {len}");
            // NB interesting that with converting double to decimal savings go from 65% to 85%,
            // even calculated from (8+8) base size not decimal's 16 size
            Console.WriteLine($"Savings: {1.0 - ((len * 1.0) / (sm.Count * 8.0))}");
            SortedMap<int, int> sm2 = null;
            var len3 = BinarySerializer.Read(buffer, out sm2);

            Assert.AreEqual(len, len3);

            Assert.IsTrue(sm2.Keys.SequenceEqual(sm.Keys));
            Assert.IsTrue(sm2.Values.SequenceEqual(sm.Values));
        }

        [Test]
        public unsafe void CouldSerializeSortedMapWithStrings()
        {
            var rng = new Random();

            var dest = (Memory<byte>)new byte[10000000];
            var buffer = dest;
            var handle = buffer.Pin();
            var ptr = (IntPtr)handle.Pointer;

            var valueLens = 0;
            var sm = new SortedMap<int, string>();
            for (var i = 0; i < 100000; i++)
            {
                var str = i.ToString();
                valueLens += str.Length;
                sm.Add(i, str);
            }
            MemoryStream temp;
            var len = BinarySerializer.SizeOf(sm, out temp);
            var len2 = BinarySerializer.Write(sm, ref buffer, temp);
            Assert.AreEqual(len, len2);
            var usefulLen = sm.Count * 4 + valueLens;
            Console.WriteLine($"Useful: {usefulLen}");
            Console.WriteLine($"Total: {len}");
            // NB interesting that with converting double to decimal savings go from 65% to 85%,
            // even calculated from (8+8) base size not decimal's 16 size
            Console.WriteLine($"Savings: {1.0 - ((len * 1.0) / (usefulLen))}");
            SortedMap<int, string> sm2 = null;
            var len3 = BinarySerializer.Read(buffer, out sm2);

            Assert.AreEqual(len, len3);

            Assert.IsTrue(sm2.Keys.SequenceEqual(sm.Keys));
            Assert.IsTrue(sm2.Values.SequenceEqual(sm.Values));
        }

        [Test]
        public void CouldUseBloscCompression()
        {
            var count = 1000;
            var rng = new Random(42);
            Memory<byte> bytes = new byte[count * 16 + 20];
            var source = new decimal[count];
            source[0] = 123.4567M;
            for (var i = 1; i < count; i++)
            {
                source[i] = source[i - 1] + Math.Round((decimal)rng.NextDouble() * 10, 4);
            }

            var len = BinarySerializer.Write(source, ref bytes, null,
                SerializationFormat.BinaryLz4);

            Console.WriteLine($"Useful: {source.Length * 16}");
            Console.WriteLine($"Total: {len}");

            decimal[] destination = null;

            var len2 = BinarySerializer.Read(bytes, out destination);

            Console.WriteLine("len2: " + len2);
            Console.WriteLine(destination.Length.ToString());
            //foreach (var val in destination)
            //{
            //    Console.WriteLine(val.ToString());
            //}

            Assert.True(source.SequenceEqual(destination));
        }

        [Test]
        public void CouldUseBloscCompressionZstd()
        {
            var count = 1000;
            var rng = new Random();
            Memory<byte> bytes = new byte[count * 16 + 20];
            var source = new decimal[count];
            source[0] = 123.4567M;
            for (var i = 1; i < count; i++)
            {
                source[i] = source[i - 1] + Math.Round((decimal)rng.NextDouble() * 10, 4);
            }

            var len = BinarySerializer.Write(source, ref bytes, null,
                SerializationFormat.BinaryZstd);

            Console.WriteLine($"Useful: {source.Length * 16}");
            Console.WriteLine($"Total: {len}");

            decimal[] destination = null;

            var len2 = BinarySerializer.Read(bytes, out destination);

            Console.WriteLine("len2: " + len2);
            Console.WriteLine(destination.Length.ToString());
            foreach (var val in destination)
            {
                Console.WriteLine(val.ToString());
            }

            Assert.True(source.SequenceEqual(destination));
        }

        public struct Dummy
        {
            public long ValL;
            public string ValS;
            public Timestamp Timestamp;
        }

        [Test]
        public void CouldSerializeTimestampAsAFieldDummyStruct()
        {
            var ptr = Marshal.AllocHGlobal(1000);

            var val = new Dummy()
            {
                Timestamp = TimeService.Default.CurrentTime,
                ValL = 123,
                ValS = "foo"
            };
            var ts = val.Timestamp;

            var serializationFormats = Enum.GetValues(typeof(SerializationFormat)).Cast<SerializationFormat>();

            var tss = new[] { default, ts };

            foreach (var timestamp in tss)
            {
                foreach (var serializationFormat in serializationFormats)
                {
                    var len = BinarySerializer.SizeOf(val, out var stream, serializationFormat, timestamp);
                    var len2 = BinarySerializer.WriteUnsafe(val, ptr, stream, serializationFormat,
                        timestamp);

                    Assert.AreEqual(len, len2);

                    var len3 = BinarySerializer.Read(ptr, out Dummy val2, out var ts2);

                    Assert.AreEqual(len, len3);
                    Assert.AreEqual(val.Timestamp, val2.Timestamp);
                    Assert.AreEqual(val.ValL, val2.ValL);
                    Assert.AreEqual(val.ValS, val2.ValS);
                    Assert.AreEqual(timestamp, ts2);
                }
            }

            var str = JsonSerializer.ToJsonString(val);
            Console.WriteLine(str);

            var str2 = JsonSerializer.ToJsonString(val.Timestamp);
            Console.WriteLine(str2);
        }

        [StructLayout(LayoutKind.Sequential, Size = 16)]
        public struct DummyBlittable
        {
            public long ValL;
            public Timestamp Timestamp;
        }

        [Test]
        public void CouldSerializeTimestampAsAFieldDummyBlittableStruct()
        {
            var ptr = Marshal.AllocHGlobal(1000);

            var val = new DummyBlittable()
            {
                Timestamp = TimeService.Default.CurrentTime,
                ValL = 123,
            };
            var ts = val.Timestamp;

            var serializationFormats = Enum.GetValues(typeof(SerializationFormat)).Cast<SerializationFormat>();

            var tss = new[] { default, ts };

            foreach (var timestamp in tss)
            {
                foreach (var serializationFormat in serializationFormats)
                {
                    var len = BinarySerializer.SizeOf(val, out var stream, serializationFormat, timestamp);
                    var len2 = BinarySerializer.WriteUnsafe(val, ptr, stream, serializationFormat,
                        timestamp);

                    Assert.AreEqual(len, len2);

                    var len3 = BinarySerializer.Read(ptr, out DummyBlittable val2, out var ts2);

                    Assert.AreEqual(len, len3);
                    Assert.AreEqual(val.Timestamp, val2.Timestamp);
                    Assert.AreEqual(val.ValL, val2.ValL);
                    Assert.AreEqual(timestamp, ts2);
                }
            }

            var str = JsonSerializer.ToJsonString(val);
            Console.WriteLine(str);

            var str2 = JsonSerializer.ToJsonString(val.Timestamp);
            Console.WriteLine(str2);
        }

        [Test]
        public void CouldSerializeStringWithTimeStamp()
        {
            var ptr = Marshal.AllocHGlobal(1000);

            var val = "bar";
            var ts = TimeService.Default.CurrentTime;

            var serializationFormats = new[] {SerializationFormat.Json}; // Enum.GetValues(typeof(SerializationFormat)).Cast<SerializationFormat>();

            var tss = new[] { default, ts };

            foreach (var timestamp in tss)
            {
                foreach (var serializationFormat in serializationFormats)
                {
                    var len = BinarySerializer.SizeOf(val, out var stream, serializationFormat, timestamp);
                    var len2 = BinarySerializer.WriteUnsafe(val, ptr, stream, serializationFormat,
                        timestamp);

                    Assert.AreEqual(len, len2);

                    var len3 = BinarySerializer.Read(ptr, out string val2, out var ts2);

                    Assert.AreEqual(len, len3);
                    Assert.AreEqual(val, val2);
                    Assert.AreEqual(timestamp, ts2);
                }
            }
        }

        [Test]
        public void CouldSerializePrimitiveWithTimeStamp()
        {
            var ptr = Marshal.AllocHGlobal(1000);

            var val = "bar";
            var ts = TimeService.Default.CurrentTime;

            var serializationFormats = new[] { SerializationFormat.Json }; // Enum.GetValues(typeof(SerializationFormat)).Cast<SerializationFormat>();

            var tss = new[] { default, ts };

            foreach (var timestamp in tss)
            {
                foreach (var serializationFormat in serializationFormats)
                {
                    var len = BinarySerializer.SizeOf(val, out var stream, serializationFormat, timestamp);
                    var len2 = BinarySerializer.WriteUnsafe(val, ptr, stream, serializationFormat,
                        timestamp);

                    Assert.AreEqual(len, len2);

                    var len3 = BinarySerializer.Read(ptr, out string val2, out var ts2);

                    Assert.AreEqual(len, len3);
                    Assert.AreEqual(val, val2);
                    Assert.AreEqual(timestamp, ts2);
                }
            }
        }

        [Test]
        public void CouldSerializePrimitiveArrayWithTimeStamp()
        {
            var ptr = Marshal.AllocHGlobal(1000);

            var val = new[] {1, 2, 3};
            var ts = TimeService.Default.CurrentTime;

            var serializationFormats = Enum.GetValues(typeof(SerializationFormat)).Cast<SerializationFormat>();

            var tss = new[] { default, ts };

            foreach (var timestamp in tss)
            {
                foreach (var serializationFormat in serializationFormats)
                {
                    var len = BinarySerializer.SizeOf(val, out var stream, serializationFormat, timestamp);
                    var len2 = BinarySerializer.WriteUnsafe(val, ptr, stream, serializationFormat,
                        timestamp);

                    Assert.AreEqual(len, len2);

                    var len3 = BinarySerializer.Read(ptr, out int[] val2, out var ts2);

                    Assert.AreEqual(len, len3);
                    Assert.IsTrue(val.SequenceEqual(val2));
                    Assert.AreEqual(timestamp, ts2);
                }
            }
        }

        [Test]
        public void CouldSerializeDummyArrayWithTimeStamp()
        {
            var ptr = Marshal.AllocHGlobal(1000);

            var val = new[] { new Dummy() {ValL = 1}, new Dummy() { ValL = 2 }};
            var ts = TimeService.Default.CurrentTime;

            var serializationFormats = Enum.GetValues(typeof(SerializationFormat)).Cast<SerializationFormat>();

            var tss = new[] { default, ts };

            foreach (var timestamp in tss)
            {
                foreach (var serializationFormat in serializationFormats)
                {
                    var len = BinarySerializer.SizeOf(val, out var stream, serializationFormat, timestamp);
                    var len2 = BinarySerializer.WriteUnsafe(val, ptr, stream, serializationFormat,
                        timestamp);

                    Assert.AreEqual(len, len2);

                    var len3 = BinarySerializer.Read(ptr, out Dummy[] val2, out var ts2);

                    Assert.AreEqual(len, len3);
                    Assert.IsTrue(val.SequenceEqual(val2));
                    Assert.AreEqual(timestamp, ts2);
                }
            }
        }

        [Test]
        public void CouldSerializeTKVWithTimeStamp()
        {
            var ptr = Marshal.AllocHGlobal(1000);

            var val = new TaggedKeyValue<int, long>(1, 2, 1);
            var ts = TimeService.Default.CurrentTime;

            var serializationFormats = Enum.GetValues(typeof(SerializationFormat)).Cast<SerializationFormat>();

            var tss = new[] { default, ts };

            foreach (var timestamp in tss)
            {
                foreach (var serializationFormat in serializationFormats)
                {
                    var len = BinarySerializer.SizeOf(val, out var stream, serializationFormat, timestamp);
                    var len2 = BinarySerializer.WriteUnsafe(val, ptr, stream, serializationFormat,
                        timestamp);

                    Assert.AreEqual(len, len2);

                    var len3 = BinarySerializer.Read(ptr, out TaggedKeyValue<int, long> val2, out var ts2);

                    Assert.AreEqual(len, len3);
                    Assert.AreEqual(val, val2);
                    Assert.AreEqual(timestamp, ts2);
                }
            }
        }
    }
}