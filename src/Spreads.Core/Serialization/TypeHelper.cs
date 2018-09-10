// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using Spreads.DataTypes;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static System.Runtime.CompilerServices.Unsafe;

namespace Spreads.Serialization
{
    internal delegate int FromPtrDelegate(IntPtr ptr, out object value);

    internal delegate int ToPtrDelegate(object value, IntPtr destination, MemoryStream ms = null, SerializationFormat compression = SerializationFormat.Binary);

    internal delegate int SizeOfDelegate(object value, out MemoryStream memoryStream, SerializationFormat compression = SerializationFormat.Binary);

    internal class TypeParams
    {
        public int Size;

        /// <summary>
        /// CLR definition, we cache it here since ty.IsValueType is a virtual call
        /// </summary>
        public bool IsValueType;

        /// <summary>
        /// Either CLR-primitive or a pinnale struct marked with SerializationAttribute(BlittableSize > 0)
        /// </summary>
        public bool IsBlittable;

        public bool IsFixedSize;
        public bool IsDateTime;
    }

    public class TypeHelper
    {
        private static readonly Dictionary<Type, FromPtrDelegate> FromPtrDelegateCache = new Dictionary<Type, FromPtrDelegate>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static FromPtrDelegate GetFromPtrDelegate(Type ty)
        {
            FromPtrDelegate temp;
            if (FromPtrDelegateCache.TryGetValue(ty, out temp)) return temp;
            var mi = typeof(TypeHelper).GetMethod("ReadObject", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
            var genericMi = mi.MakeGenericMethod(ty);
            temp = (FromPtrDelegate)genericMi.CreateDelegate(typeof(FromPtrDelegate));
            FromPtrDelegateCache[ty] = temp;
            return temp;
        }

        private static readonly Dictionary<Type, ToPtrDelegate> ToPtrDelegateCache = new Dictionary<Type, ToPtrDelegate>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ToPtrDelegate GetToPtrDelegate(Type ty)
        {
            ToPtrDelegate temp;
            if (ToPtrDelegateCache.TryGetValue(ty, out temp)) return temp;
            var mi = typeof(TypeHelper).GetMethod("WriteObject", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
            var genericMi = mi.MakeGenericMethod(ty);
            temp = (ToPtrDelegate)genericMi.CreateDelegate(typeof(ToPtrDelegate));
            ToPtrDelegateCache[ty] = temp;
            return temp;
        }

        private static readonly Dictionary<Type, SizeOfDelegate> SizeOfDelegateCache = new Dictionary<Type, SizeOfDelegate>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static SizeOfDelegate GetSizeOfDelegate(Type ty)
        {
            SizeOfDelegate temp;
            if (SizeOfDelegateCache.TryGetValue(ty, out temp)) return temp;
            var mi = typeof(TypeHelper).GetMethod("SizeOfObject", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
            var genericMi = mi.MakeGenericMethod(ty);
            temp = (SizeOfDelegate)genericMi.CreateDelegate(typeof(SizeOfDelegate));
            SizeOfDelegateCache[ty] = temp;
            return temp;
        }

        private static readonly Dictionary<Type, int> SizeDelegateCache = new Dictionary<Type, int>();

        // used by reflection below
        private static int Size<T>()
        {
            return TypeHelper<T>.FixedSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetSize(Type ty)
        {
            int temp;
            if (SizeDelegateCache.TryGetValue(ty, out temp)) return temp;
            var mi = typeof(TypeHelper).GetMethod("Size", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
            var genericMi = mi.MakeGenericMethod(ty);
            temp = (int)genericMi.Invoke(null, new object[] { });
            SizeDelegateCache[ty] = temp;
            return temp;
        }
    }

    // NB Fail: static RO are not JIT consts for generics!

    public sealed unsafe class TypeHelper<T> : TypeHelper
    {
        // Default header + value
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        internal struct Placeholder
        {
            public readonly DataTypeHeader Header;
            public T Value;

            public Placeholder(DataTypeHeader header)
            {
                Header = header;
                Value = default;
            }
        }

        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        internal struct PlaceholderTS
        {
            public readonly DataTypeHeader Header;
            public Timestamp Timestamp;
            public T Value;

            public PlaceholderTS(DataTypeHeader header)
            {
                Header = header;
                Timestamp = default;
                Value = default;
            }
        }

        // ReSharper disable StaticMemberInGenericType
        private static bool _hasBinaryConverter;

        public static bool HasBinaryConverter
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _hasBinaryConverter;
        }

        /// <summary>
        /// Returns a positive size of a pinnable type T, -1 if the type T is not pinnable or has
        /// a registered <see cref="IBinaryConverter{T}"/> converter.
        /// We assume the type T is pinnable if `GCHandle.Alloc(T[2], GCHandleType.Pinned) = true`.
        /// This is more relaxed than Marshal.SizeOf, but still doesn't cover cases such as
        /// an array of KVP[DateTime,double], which has contiguous layout in memory.
        /// </summary>
        public static readonly int FixedSize = InitChecked();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int EnsureFixedSize()
        {
            if (FixedSize > 0)
            {
                return FixedSize;
            }

            ThrowTypeIsNotFixedSize();
            return -1;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowTypeIsNotFixedSize()
        {
            throw new ApplicationException($"Type {typeof(T).Name} is not fixed size. Either add Size parameter to StructLayout attribute or use Spreads.Serialization attribute to explicitly opt-in to treat non-primitive user-defined structs as fixed-size.");
        }

        internal static readonly bool IsValueType = typeof(T).GetTypeInfo().IsValueType;
        internal static readonly bool IsIDelta = typeof(IDelta<T>).GetTypeInfo().IsAssignableFrom(typeof(T));

        /// <summary>
        /// True if an array T[] could be pinned in memory.
        /// </summary>
        internal static readonly bool IsPinnable = FixedSize > 0 && typeof(T) != typeof(DateTime);

        private static IBinaryConverter<T> _converterInstance;
        private static TypeParams _typeParams;

        // ReSharper disable once InconsistentNaming
        private static readonly Placeholder _placeholder = new Placeholder(new DataTypeHeader
        {
            VersionAndFlags =
            {
                Version = 0,
                IsBinary = true,
                IsDelta = false,
                IsCompressed = false
            },
            TypeEnum = VariantHelper<T>.TypeEnum,
            TypeSize = (FixedSize > 0 && FixedSize <= 255) ? (byte)FixedSize : (byte)0
        });

        private static readonly PlaceholderTS _placeholderTs = new PlaceholderTS(new DataTypeHeader
        {
            VersionAndFlags =
            {
                Version = 0,
                IsBinary = true,
                IsDelta = false,
                IsCompressed = false,
                IsTimestamped = true
            },
            TypeEnum = VariantHelper<T>.TypeEnum,
            TypeSize = (FixedSize > 0 && FixedSize <= 255) ? (byte)FixedSize : (byte)0
        });

        // ReSharper restore StaticMemberInGenericType

        // Just in case, do not use static ctor in any critical paths: https://github.com/Spreads/Spreads/issues/66
        // static TypeHelper() { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int PinnedSize()
        {
            try
            {
                var array = new T[2];
                var pinnedArrayHandle = GCHandle.Alloc(array, GCHandleType.Pinned);
                var size = (int)
                    (Marshal.UnsafeAddrOfPinnedArrayElement(array, 1).ToInt64() -
                     Marshal.UnsafeAddrOfPinnedArrayElement(array, 0).ToInt64());
                pinnedArrayHandle.Free();
                // Type helper works only with types that could be pinned in arrays
                // Here we just cross-check, happens only in static constructor
                var unsafeSize = System.Runtime.CompilerServices.Unsafe.SizeOf<T>();
                if (unsafeSize != size) { Environment.FailFast("Pinned and unsafe sizes differ!"); }
                return size;
            }
            catch
            {
                return -1;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int InitChecked()
        {
            try
            {
                var size = Init();
                // NB do not support huge blittable type
                return size < 256 ? size : -1;
            }
            catch
            {
                return -1;
            }
        }

        /// <summary>
        /// Method is only called from the static constructor of TypeHelper.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Init()
        {
            _typeParams = new TypeParams();
            if (typeof(T) == typeof(DateTime))
            {
                _typeParams.IsBlittable = false;
                _typeParams.IsFixedSize = true;
                _typeParams.IsDateTime = true;
                _typeParams.Size = 8;
                return 8;
            }
            // NB decimal is pinnable but not primitive, the check below fails on it
            if (typeof(T) == typeof(decimal))
            {
                _typeParams.IsBlittable = true;
                _typeParams.IsFixedSize = true;
                _typeParams.IsDateTime = false;
                _typeParams.Size = 16;
                return 16;
            }

            _typeParams.IsValueType = typeof(T).GetTypeInfo().IsValueType;
            var pinnedSize = PinnedSize();

            if (pinnedSize > 0)
            {
                if (typeof(T).GetTypeInfo().IsPrimitive && typeof(T) != typeof(bool))
                {
                    _typeParams.IsBlittable = true;
                    _typeParams.IsFixedSize = true;
                    _typeParams.Size = pinnedSize;
                    return pinnedSize;
                }

                // for a non-primitive type to be blittable, it must have an attribute
                var sa = SerializationAttribute.GetSerializationAttribute(typeof(T));
                var hasSizeAttribute = false;
                if (sa != null && sa.BlittableSize > 0)
                {
                    if (pinnedSize != sa.BlittableSize)
                    {
                        Environment.FailFast(
                            $"Size of type {typeof(T).Name} defined in SerializationAttribute {sa.BlittableSize} differs from calculated size {pinnedSize}.");
                    }
                    hasSizeAttribute = true;
                }
                else
                {
                    var sla = SerializationAttribute.GetStructLayoutAttribute(typeof(T));
                    if (sla != null && sla.Size > 0)
                    {
                        if (pinnedSize != sla.Size || sla.Value == LayoutKind.Auto)
                        {
                            Environment.FailFast(
                                $"Size of type {typeof(T).Name} defined in StructLayoutAttribute {sla.Size} differs from calculated size {pinnedSize} or layout is set to LayoutKind.Auto.");
                        }
                        hasSizeAttribute = true;
                    }
                }
                if (hasSizeAttribute)
                {
                    if (typeof(IBinaryConverter<T>).IsAssignableFrom(typeof(T)))
                    {
                        // NB: this makes no sense, because blittable is version 0, if we have any change
                        // to struct layout later, we won't be able to work with version 0 anymore
                        // and will lose ability to work with old values.
                        Environment.FailFast($"Blittable types must not implement IBinaryConverter<T> interface.");
                    }
                    _typeParams.IsBlittable = true;
                    _typeParams.IsFixedSize = true;
                    _typeParams.Size = pinnedSize;
                    return pinnedSize;
                }
                if (sa != null && sa.PreferBlittable)
                {
                    // NB: here it is OK to have an interface, we just opt-in for blittable
                    // when we know it won't change, e.g. generic struct with fixed fields (KV<K,V>, DictEntry<K,V>, Message<T>, etc.)
                    // usually those types are internal
                    _typeParams.IsBlittable = true;
                    _typeParams.IsFixedSize = true;
                    _typeParams.Size = pinnedSize;
                    return pinnedSize;
                }
            }

            // by this line the type is not blittable
            _typeParams.IsBlittable = false;

            // NB we try to check interface as a last step, because some generic types
            // could implement IBinaryConverter<T> but still be blittable for certain types,
            // e.g. DateTime vs long in PersistentMap<K,V>.Entry
            //if (tmp is IBinaryConverter<T>) {
            if (typeof(IBinaryConverter<T>).IsAssignableFrom(typeof(T)))
            {
                IBinaryConverter<T> converter = null;
                try
                {
                    converter = (IBinaryConverter<T>)Activator.CreateInstance<T>();
                }
                catch
                {
                    //Trace.TraceWarning($"Type {typeof(T).FullName} is marked as IBinaryConverter and so it must have a parameterless constructor");
                    Environment.FailFast($"Type T ({typeof(T).FullName}) is marked as IBinaryConverter<T> and therefore must have a parameterless constructor.");
                }
                if (converter.ConverterVersion <= 0)
                {
                    Environment.FailFast("User IBinaryConverter<T> implementation for a type T should have a positive version.");
                }
                _converterInstance = converter;
                _hasBinaryConverter = true;
                return _converterInstance.IsFixedSize ? _converterInstance.Size : -1;
            }

            if (typeof(T) == typeof(string))
            {
                _converterInstance = (IBinaryConverter<T>)(new StringBinaryConverter());
                _hasBinaryConverter = true;
            }

            if (typeof(T).IsArray)
            {
                var elementType = typeof(T).GetElementType();
                var elementSize = GetSize(elementType);
                if (elementSize > 0)
                { // only for blittable types
                    var converter = (IBinaryConverter<T>)ArrayConverterFactory.Create(elementType);
                    if (converter == null) return -1;
                    _converterInstance = converter;
                    _hasBinaryConverter = true;
                    Trace.Assert(!_converterInstance.IsFixedSize);
                    Trace.Assert(_converterInstance.Size == -1);
                }
            }
            return -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Read(IntPtr ptr, out T value, out Timestamp timestamp)
        {
            if (_hasBinaryConverter)
            {
                Debug.Assert(FixedSize == -1);
                return _converterInstance.Read(ptr, out value, out timestamp);
            }
            if (FixedSize >= 0)
            {
                Debug.Assert(FixedSize > 0);
                var header = ReadUnaligned<DataTypeHeader>((void*)(ptr));
                var tsSize = 0;
                if (header.VersionAndFlags.IsTimestamped)
                {
                    tsSize = Timestamp.Size;
                    timestamp = ReadUnaligned<Timestamp>((void*)(ptr + DataTypeHeader.Size));
                }
                else
                {
                    timestamp = default;
                }
                value = ReadUnaligned<T>((void*)(ptr + DataTypeHeader.Size + tsSize));
                return DataTypeHeader.Size + tsSize + FixedSize;
            }
            Debug.Assert(FixedSize < 0);
            THFFCannotReadVarSize();
            value = default;
            timestamp = default;
            return -1;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        // ReSharper disable once InconsistentNaming
        private static void THFFCannotReadVarSize()
        {
            ThrowHelper.FailFast(
                "TypeHelper<T> doesn't support variable-size types. Code calling this method is incorrect, data corruption is possible.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Write(in T value, IntPtr destination, MemoryStream ms = null,
            SerializationFormat format = SerializationFormat.Binary,
            Timestamp timestamp = default)
        {
            if (_hasBinaryConverter)
            {
                return _converterInstance.Write(value, destination, ms, format, timestamp);
            }
            if (FixedSize >= 0)
            {
                var tsSize = 0;
                if (timestamp != default)
                {
                    tsSize = Timestamp.Size;
                    // copy by value
                    var placeholder = _placeholderTs;
                    placeholder.Timestamp = timestamp;
                    placeholder.Value = value;
                    WriteUnaligned((void*)(destination), placeholder);
                }
                else
                {
                    // copy by value
                    var placeholder = _placeholder;
                    placeholder.Value = value;
                    WriteUnaligned((void*)(destination), placeholder);
                }
                Debug.Assert(FixedSize > 0);

                var len = DataTypeHeader.Size + tsSize + FixedSize;
                return len;
            }

            Debug.Assert(FixedSize < 0);
            ThrowHelper.FailFast("TypeHelper<T> doesn't support variable-size types. Code calling this method is incorrect, data corruption is possible.");
            return -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int WriteFixedSize(in T value, IntPtr destination)
        {
            var len = DataTypeHeader.Size + FixedSize;
            // copy by value
            var placeholder = _placeholder;
            placeholder.Value = value;
            WriteUnaligned((void*)(destination), placeholder);
            return len;
        }

        /// <summary>
        /// Returns binary size of the value with header
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int SizeOf(T value, out MemoryStream memoryStream,
            SerializationFormat format,
            Timestamp timestamp = default)
        {
            memoryStream = null;
            if (_hasBinaryConverter)
            {
                Debug.Assert(FixedSize == -1);
                return _converterInstance.SizeOf(value, out memoryStream, format, timestamp);
            }
            if (FixedSize >= 0)
            {
                return DataTypeHeader.Size + FixedSize + ((long)timestamp == default ? 0 : Timestamp.Size);
            }
            Debug.Assert(FixedSize < 0);
            return -1;
        }

        public static byte ConverterVersion
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _hasBinaryConverter ? _converterInstance.ConverterVersion : (byte) 0;
        }

        public static void RegisterConverter(IBinaryConverter<T> converter,
            bool overrideExisting = false)
        {
            if (converter == null) { throw new ArgumentNullException(nameof(converter)); }
            if (FixedSize >= 0) { throw new InvalidOperationException("Cannot register a custom converter for pinnable types"); }

            // NB TypeHelper is internal, we could provide some hooks later e.g. for char or bool
            if (converter.ConverterVersion == 0 || converter.ConverterVersion > 15)
            {
                throw new ArgumentException("User-implemented converter version must be in the range 1-15.");
            }

            if (_hasBinaryConverter && !overrideExisting)
            {
                throw new InvalidOperationException(
                    $"Type {typeof(T)} already implements IBinaryConverter<{typeof(T)}> interface. Use versioning to add a new converter (not supported yet)");
            }

            if (_typeParams.IsBlittable)
            {
                Environment.FailFast($"Blittable types must not have IBinaryConverter<T>.");
            }
            _hasBinaryConverter = true;
            _converterInstance = converter;
        }
    }
}