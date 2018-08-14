// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// using Newtonsoft.Json;
using Spreads.Buffers;
using Spreads.DataTypes;
using Spreads.Serialization.Utf8Json;
using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using static System.Runtime.CompilerServices.Unsafe;

#pragma warning disable 0618

namespace Spreads.Serialization
{
    /// <summary>
    /// Binary Serializer that tries to serialize objects to their blittable representation whenever possible
    /// and falls back to JSON.NET for non-blittable types. It supports versioning and custom binary converters.
    /// </summary>
    public static class BinarySerializer
    {
        /// <summary>
        /// Positive number for fixed-size types, zero for types with a custom binary converters, negative for all other types.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Size<T>()
        {
            return TypeHelper<T>.Size;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int SizeOf<T>(T value, out MemoryStream temporaryStream,
            SerializationFormat format = SerializationFormat.Binary, bool skipHeader = false)
        {
            return SizeOf(in value, out temporaryStream, format, skipHeader);
        }

        /// <summary>
        /// Binary size of value T after serialization.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int SizeOf<T>(in T value, out MemoryStream temporaryStream,
            SerializationFormat format = SerializationFormat.Binary, bool skipHeader = false)
        {
            if ((int)format < 100)
            {
                var size = TypeHelper<T>.SizeOf(value, out temporaryStream, format, skipHeader);
                if (size >= 0)
                {
                    return size;
                }
            }

            return SizeOfSlow(value, out temporaryStream, format, skipHeader);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static int SizeOfSlow<T>(T value, out MemoryStream temporaryStream, SerializationFormat format,
            bool skipHeader)
        {
            var prefixLength = skipHeader ? 4 : DataTypeHeader.Size + 4;

            // NB when we request binary uncompressed but items are not fixed size we use uncompressed
            // JSON so that we could iterate over values as byte Spans/DirectBuffers without uncompressing
            if (format == SerializationFormat.Json || format == SerializationFormat.Binary)
            {
                var rms = JsonSerializer.SerializeWithOffset(value, prefixLength);
                rms.Position = 0;
                if (!skipHeader)
                {
                    var header = new DataTypeHeader
                    {
                        // NB All defaults
                        //VersionAndFlags =
                        //{
                        //    // Version = 0,
                        //    // IsBinary = false,
                        //    // IsDelta = false,
                        //    // IsCompressed = false
                        //},
                        TypeEnum = VariantHelper<T>.TypeEnum
                    };
                    rms.WriteAsPtr(header);
                }

                rms.WriteAsPtr(checked((int)rms.Length - prefixLength));

                rms.Position = 0;
                temporaryStream = rms;
                return checked((int)rms.Length);
            }
            else
            {
                // NB: fallback for failed compressed binary uses Json.Deflate

                // uncompressed
                var rms = JsonSerializer.SerializeWithOffset(value, 0);
                var compressedStream =
                    RecyclableMemoryStreamManager.Default.GetStream(null, checked((int)rms.Length));

                compressedStream.SetLengthInternal(prefixLength);
                compressedStream.PositionInternal = prefixLength;

                using (var compressor = new DeflateStream(compressedStream, CompressionLevel.Optimal, true))
                {
                    rms.Position = 0;
                    rms.CopyTo(compressor);
                    compressor.Dispose();
                }

                rms.Dispose();

                compressedStream.Position = 0;

                if (!skipHeader)
                {
                    var header = new DataTypeHeader
                    {
                        VersionAndFlags =
                        {
                            // NB Do not assign defaults
                            // Version = 0,
                            // IsBinary = false,
                            // IsDelta = false,
                            IsCompressed = true
                        },
                        TypeEnum = VariantHelper<T>.TypeEnum
                    };
                    compressedStream.WriteAsPtr(header);
                }

                compressedStream.WriteAsPtr(checked((int)compressedStream.Length - prefixLength));

                compressedStream.Position = 0;
                temporaryStream = compressedStream;
                return (checked((int)compressedStream.Length));
            }
        }

        /// <summary>
        /// Destination must be pinned and have enough size.
        /// Unless writing to an "endless" buffer SizeOf must be called
        /// first to determine the size and prepare destination buffer.
        /// For blittable types (Size >= 0) this method add 4 bytes header unless skipHeader is true.
        /// Use Unsafe.WriteUnaligned() to write blittable types directly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int WriteUnsafe<T>(in T value, IntPtr pinnedDestination,
            MemoryStream temporaryStream = null,
            SerializationFormat format = SerializationFormat.Binary,
            bool skipHeader = false)
        {
            if (TypeHelper<T>.Size >= 0 && (int)format < 100)
            {
                Debug.Assert(temporaryStream == null, "For primitive types MemoryStream should not be used");
                return TypeHelper<T>.Write(value, pinnedDestination, null, format, skipHeader);
            }
            return WriteSlow(in value, pinnedDestination, temporaryStream, format, skipHeader);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static unsafe int WriteSlow<T>(in T value, IntPtr pinnedDestination,
            MemoryStream temporaryStream,
            SerializationFormat format, bool skipHeader)
        {
            if (value == null)
            {
                ThrowHelper.ThrowArgumentNullException(nameof(value));
            }

            if (temporaryStream != null)
            {
                Debug.Assert(temporaryStream.Position == 0);
#if DEBUG
                var checkSize = SizeOf(value, out MemoryStream tmp, format, skipHeader);
                Debug.Assert(checkSize == temporaryStream.Length, "Memory stream length must be equal to the SizeOf");
                tmp?.Dispose();
#endif
                var size = checked((int)temporaryStream.Length);
                temporaryStream.WriteToRef(ref AsRef<byte>((void*)pinnedDestination));
                temporaryStream.Dispose();
                return size;
            }

            if ((int)format < 100)
            {
                if (TypeHelper<T>.Size > 0 || TypeHelper<T>.HasBinaryConverter)
                {
                    return TypeHelper<T>.Write(value, pinnedDestination, null, format, skipHeader);
                }
            }

            SizeOf(in value, out temporaryStream, format, skipHeader);
            if (temporaryStream == null)
            {
                ThrowHelper.ThrowInvalidOperationException("Tempstream for Json or binary fallback must be returned from SizeOf");
            }

            return WriteSlow(in value, pinnedDestination, temporaryStream, format, skipHeader);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Write<T>(T value, ref byte[] destination,
            MemoryStream temporaryStream = null,
            SerializationFormat format = SerializationFormat.Binary,
            bool skipHeader = false)
        {
            var asMemory = (Memory<byte>)destination;
            return Write(in value, ref asMemory, temporaryStream, format, skipHeader);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Write<T>(T value, ref Memory<byte> destination,
            MemoryStream temporaryStream = null,
            SerializationFormat format = SerializationFormat.Binary,
            bool skipHeader = false)
        {
            return Write(in value, ref destination, temporaryStream, format, skipHeader);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe int Write<T>(in T value, ref Memory<byte> destination,
            MemoryStream temporaryStream = null,
            SerializationFormat format = SerializationFormat.Binary,
            bool skipHeader = false)
        {
            var capacity = destination.Length;
            int size;
            if (temporaryStream == null)
            {
                size = SizeOf(in value, out temporaryStream, format, skipHeader);
            }
            else
            {
                size = checked((int)temporaryStream.Length);
                if (temporaryStream.Length > capacity)
                {
                    ThrowHelper.ThrowInvalidOperationException("desctination doesn't have enough size");
                }

                temporaryStream.WriteToRef(ref destination.Span[0]);
            }

            if (size > capacity)
            {
                ThrowHelper.ThrowInvalidOperationException("desctination doesn't have enough size");
            }

            if (temporaryStream != null)
            {
                Debug.Assert(temporaryStream.Length == size);
                temporaryStream.WriteToRef(ref destination.Span[0]);
                temporaryStream.Dispose();
                return size;
            }

            fixed (void* ptr = &destination.Span[0])
            {
                return WriteUnsafe(value, (IntPtr)ptr, null, format, skipHeader);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe int Read<T>(IntPtr ptr, out T value)
        {
            var position = 0;

            var header = ReadUnaligned<DataTypeHeader>((void*)ptr);

            if (header.VersionAndFlags.IsBinary || TypeHelper<T>.HasBinaryConverter)
            {
                Debug.Assert(TypeHelper<T>.Size >= 0 || TypeHelper<T>.HasBinaryConverter);
                return TypeHelper<T>.Read(ptr, out value);
            }

            var payloadSize = ReadUnaligned<int>((void*)(ptr + DataTypeHeader.Size));
            return ReadSlow(ptr, out value, header, payloadSize, false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe int Read<T>(IntPtr ptr, out T value, DataTypeHeader header)
        {
            var position = 0;

            if (header.VersionAndFlags.IsBinary || TypeHelper<T>.HasBinaryConverter)
            {
                Debug.Assert(TypeHelper<T>.Size >= 0 || TypeHelper<T>.HasBinaryConverter);
                return TypeHelper<T>.Read(ptr, out value, true);
            }

            var payloadSize = ReadUnaligned<int>((void*)(ptr));
            return ReadSlow(ptr, out value, header, payloadSize, true);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static unsafe int ReadSlow<T>(IntPtr ptr, out T value, DataTypeHeader header, int payloadSize, bool noHeader)
        {
            if (header.VersionAndFlags.Version != 0)
            {
                ThrowHelper.ThrowNotImplementedException(
                    "Only version 0 is supported for unknown types that are serialized as JSON");
                value = default;
                return -1;
            }

            var prefixLength = noHeader ? 4 : DataTypeHeader.Size + 4;

            if (!header.VersionAndFlags.IsCompressed)
            {
                var buffer = BufferPool<byte>.Rent(payloadSize);
                CopyBlockUnaligned(ref buffer[0], ref *(byte*)(ptr + prefixLength), (uint)payloadSize);
                var rms = RecyclableMemoryStream.Create(RecyclableMemoryStreamManager.Default, null,
                    payloadSize, buffer, payloadSize);
                value = JsonSerializer.Deserialize<T>(rms);
                rms.Dispose();
                return prefixLength + payloadSize;
            }
            else
            {
                return ReadJsonCompressed(ptr, out value, payloadSize, noHeader);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static unsafe int ReadJsonCompressed<T>(IntPtr ptr, out T value, int payloadSize, bool noHeader)
        {
            var prefixLength = noHeader ? 4 : DataTypeHeader.Size + 4;

            // TODO rent from RMS
            var buffer = BufferPool<byte>.Rent(payloadSize);
            CopyBlockUnaligned(ref buffer[0], ref *(byte*)(ptr + prefixLength), (uint)payloadSize);
            var comrpessedStream = RecyclableMemoryStream.Create(RecyclableMemoryStreamManager.Default, null,
                payloadSize, buffer, payloadSize);

            RecyclableMemoryStream decompressedStream = RecyclableMemoryStreamManager.Default.GetStream();

            using (var decompressor = new DeflateStream(comrpessedStream, CompressionMode.Decompress, true))
            {
                decompressor.CopyTo(decompressedStream);
                decompressor.Dispose();
            }

            comrpessedStream.Dispose();
            decompressedStream.Position = 0;

            value = JsonSerializer.Deserialize<T>(decompressedStream);

            decompressedStream.Dispose();

            return prefixLength + payloadSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe int Read<T>(ReadOnlyMemory<byte> buffer, out T value)
        {
            using (var handle = buffer.Pin())
            {
                return Read((IntPtr)handle.Pointer, out value);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Read<T>(Stream stream, out T value)
        {
            if (stream is RecyclableMemoryStream rms && rms.IsSingleChunk)
            {
                return Read(rms.SingleChunk, out value);
            }

            try
            {
                var len = checked((int)stream.Length);
                rms = RecyclableMemoryStreamManager.Default.GetStream(null, len, true);

                try
                {
                    if (!rms.IsSingleChunk)
                    {
                        ThrowHelper.ThrowInvalidOperationException(
                            "RMS GetStream(null, len, true) must return single chunk");
                    }

                    stream.CopyTo(rms);
                    return Read(rms.SingleChunk, out value);
                }
                finally
                {
                    rms.Dispose();
                }
            }
            catch (NotSupportedException)
            {
                rms = RecyclableMemoryStreamManager.Default.GetStream();
                try
                {
                    stream.CopyTo(rms);
                    return Read(rms, out value);
                }
                finally
                {
                    rms.Dispose();
                }
            }
        }
    }

#pragma warning restore 0618
}
