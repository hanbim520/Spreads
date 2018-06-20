﻿// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using System;
using NUnit.Framework;
using Spreads.Utils;
using System.Runtime.InteropServices;
using Spreads.Serialization;

namespace Spreads.Core.Tests.Serialization
{
    [TestFixture]
    public class Utf8JsonTests
    {
        [StructLayout(LayoutKind.Sequential)]
        public struct TestValue
        {
            public string Str { get; set; }
            public int Num { get; set; }
            public int Num1 { get; set; }
            public int Num2 { get; set; }
            //public Decimal Dec { get; set; }
            public double Dbl { get; set; }
            public double Dbl1 { get; set; }
        }

        [Test, Explicit("long running")]
        public void CompareUtf8JsonWithBinarySerializer()
        {
            var count = 100_000;
            var values = new TestValue[count];
            for (int i = 0; i < count; i++)
            {
                values[i] = new TestValue()
                {
                    // Dec = (((decimal)i + 1M / (decimal)(i + 1))),
                    Dbl = (double)i + 1 / (double)(i + 1),
                    Dbl1 = (double)i + 1 / (double)(i + 1),
                    Num = i,
                    Num1 = i,
                    Num2 = i,
                    Str = i.ToString()
                };
            }

            for (int r = 0; r < 50; r++)
            {
                //using (Benchmark.Run("JSON.NET", count))
                //{
                //    var lenSum = 0L;
                //    for (int i = 0; i < count; i++)
                //    {
                //        var ms = BinarySerializer.Json.Serialize(values[i]);
                //        var val = BinarySerializer.Json.Deserialize<TestValue>(ms);
                //        lenSum += ms.Length;
                //        //var str = Encoding.UTF8.GetString(ms.ToArray());
                //        //Console.WriteLine(str);
                //        ms.Dispose();
                //    }
                //    // Console.WriteLine("JSON.NET " + lenSum);
                //}

                using (Benchmark.Run("Utf8Json.NuGet", count))
                {
                    var lenSum = 0L;

                    for (int i = 0; i < count; i++)
                    {
                        var arrSegment = Utf8Json.JsonSerializer.SerializeUnsafe(values[i]);
                        Utf8Json.JsonSerializer.Deserialize<TestValue>(arrSegment.Array);
                        lenSum += arrSegment.Count;
                        //var str = Encoding.UTF8.GetString(stream.ToArray());
                        //Console.WriteLine(str);
                    }
                    // Console.WriteLine("Utf8Json " + lenSum);
                }

                using (Benchmark.Run("Utf8Json.Spreads", count))
                {
                    var lenSum = 0L;

                    for (int i = 0; i < count; i++)
                    {
                        var arrSegment = Spreads.Serialization.Utf8Json.JsonSerializer.SerializeUnsafe(values[i]);
                        Spreads.Serialization.Utf8Json.JsonSerializer.Deserialize<TestValue>(arrSegment.Array);
                        lenSum += arrSegment.Count;
                        //var str = Encoding.UTF8.GetString(stream.ToArray());
                        //Console.WriteLine(str);
                    }
                    // Console.WriteLine("Utf8Json " + lenSum);
                }
            }
            Benchmark.Dump();
        }
    }
}