using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Types;
using NUnit.Framework;
using ParquetSharp.Dataset.Filter;

namespace ParquetSharp.Dataset.Test.Filter;

[TestFixture]
public class TestArrayMaskApplier
{
    [Test]
    public void TestFilterArrays()
    {
        const int numRows = 1_001;
        var random = new Random(0);

        var bitMask = new byte[BitUtility.ByteCount(numRows)];
        for (var i = 0; i < numRows; ++i)
        {
            var included = random.NextDouble() < 0.5;
            BitUtility.SetBit(bitMask, i, included);
        }

        var mask = new FilterMask(bitMask);

        var arrays = TestArrays(numRows);

        foreach (var array in arrays)
        {
            var applier = new ArrayMaskApplier(mask);
            array.Accept(applier);
            var masked = applier.MaskedArray;

            var typeComparer = new TypeComparer(array.Data.DataType);
            masked.Data.DataType.Accept(typeComparer);
            Assert.That(typeComparer.TypesMatch, $"Masked array type {masked.Data.DataType} does not match source type {array.Data.DataType}");

            var validator = new MaskedArrayValidator(array, mask);
            masked.Accept(validator);
        }
    }

    [Test]
    public void TestIncludeAllRows()
    {
        const int numRows = 101;

        var bitMask = new byte[BitUtility.ByteCount(numRows)];
        for (var i = 0; i < numRows; ++i)
        {
            BitUtility.SetBit(bitMask, i, true);
        }

        var mask = new FilterMask(bitMask);

        var arrays = TestArrays(numRows);

        foreach (var array in arrays)
        {
            var applier = new ArrayMaskApplier(mask);
            array.Accept(applier);
            var masked = applier.MaskedArray;

            var typeComparer = new TypeComparer(array.Data.DataType);
            masked.Data.DataType.Accept(typeComparer);
            Assert.That(typeComparer.TypesMatch, $"Masked array type {masked.Data.DataType} does not match source type {array.Data.DataType}");

            Assert.That(masked.Length, Is.EqualTo(numRows));
            var validator = new MaskedArrayValidator(array, mask);
            masked.Accept(validator);
        }
    }

    [Test]
    public void TestExcludeAllRows()
    {
        const int numRows = 101;

        var bitMask = new byte[BitUtility.ByteCount(numRows)];
        for (var i = 0; i < numRows; ++i)
        {
            BitUtility.SetBit(bitMask, i, false);
        }

        var mask = new FilterMask(bitMask);

        var arrays = TestArrays(numRows);

        foreach (var array in arrays)
        {
            var applier = new ArrayMaskApplier(mask);
            array.Accept(applier);
            var masked = applier.MaskedArray;

            var typeComparer = new TypeComparer(array.Data.DataType);
            masked.Data.DataType.Accept(typeComparer);
            Assert.That(typeComparer.TypesMatch, $"Masked array type {masked.Data.DataType} does not match source type {array.Data.DataType}");

            Assert.That(masked.Length, Is.EqualTo(0));
        }
    }

    private static IArrowArray[] TestArrays(int numRows)
    {
        var random = new Random(0);

        return new IArrowArray[]
        {
            BuildArray<byte, UInt8Array, UInt8Array.Builder>(
                new UInt8Array.Builder(), numRows, random, rand => (byte)rand.NextInt64(0, 1 + byte.MaxValue)),
            BuildArray<ushort, UInt16Array, UInt16Array.Builder>(
                new UInt16Array.Builder(), numRows, random, rand => (ushort)rand.NextInt64(0, 1 + ushort.MaxValue)),
            BuildArray<uint, UInt32Array, UInt32Array.Builder>(
                new UInt32Array.Builder(), numRows, random, rand => (uint)rand.NextInt64(0, 1L + uint.MaxValue)),
            BuildArray<ulong, UInt64Array, UInt64Array.Builder>(
                new UInt64Array.Builder(), numRows, random, rand => unchecked((ulong)rand.NextInt64(long.MinValue, long.MaxValue))),
            BuildArray<sbyte, Int8Array, Int8Array.Builder>(
                new Int8Array.Builder(), numRows, random, rand => (sbyte)rand.NextInt64(sbyte.MinValue, 1 + sbyte.MaxValue)),
            BuildArray<short, Int16Array, Int16Array.Builder>(
                new Int16Array.Builder(), numRows, random, rand => (short)rand.NextInt64(short.MinValue, 1 + short.MaxValue)),
            BuildArray<int, Int32Array, Int32Array.Builder>(
                new Int32Array.Builder(), numRows, random, rand => (int)rand.NextInt64(int.MinValue, 1L + int.MaxValue)),
            BuildArray<long, Int64Array, Int64Array.Builder>(
                new Int64Array.Builder(), numRows, random, rand => rand.NextInt64(long.MinValue, long.MaxValue)),
            BuildArray<Half, HalfFloatArray, HalfFloatArray.Builder>(
                new HalfFloatArray.Builder(), numRows, random, rand => (Half)rand.NextDouble()),
            BuildArray<float, FloatArray, FloatArray.Builder>(
                new FloatArray.Builder(), numRows, random, rand => (float)rand.NextDouble()),
            BuildArray<double, DoubleArray, DoubleArray.Builder>(
                new DoubleArray.Builder(), numRows, random, rand => rand.NextDouble()),
            BuildArray<bool, BooleanArray, BooleanArray.Builder>(
                new BooleanArray.Builder(), numRows, random, rand => rand.NextDouble() < 0.5),
            BuildArray<DateOnly, Date32Array, Date32Array.Builder>(
                new Date32Array.Builder(), numRows, random, rand => DateOnly.FromDayNumber((int)rand.NextInt64(0, DateOnly.MaxValue.DayNumber))),
            BuildArray<DateOnly, Date64Array, Date64Array.Builder>(
                new Date64Array.Builder(), numRows, random, rand => DateOnly.FromDayNumber((int)rand.NextInt64(0, DateOnly.MaxValue.DayNumber))),
            BuildArray<TimeOnly, Time32Array, Time32Array.Builder>(
                new Time32Array.Builder(Apache.Arrow.Types.TimeUnit.Second), numRows, random,
                rand => TimeOnly.FromTimeSpan(TimeSpan.FromMilliseconds(rand.NextInt64(0, 1_000_000)))),
            BuildArray<TimeOnly, Time32Array, Time32Array.Builder>(
                new Time32Array.Builder(Apache.Arrow.Types.TimeUnit.Millisecond), numRows, random,
                rand => TimeOnly.FromTimeSpan(TimeSpan.FromMilliseconds(rand.NextInt64(0, 1_000_000)))),
            BuildArray<TimeOnly, Time64Array, Time64Array.Builder>(
                new Time64Array.Builder(Apache.Arrow.Types.TimeUnit.Microsecond), numRows, random,
                rand => TimeOnly.FromTimeSpan(TimeSpan.FromMilliseconds(rand.NextInt64(0, 1_000_000)))),
            BuildArray<TimeOnly, Time64Array, Time64Array.Builder>(
                new Time64Array.Builder(Apache.Arrow.Types.TimeUnit.Nanosecond), numRows, random,
                rand => TimeOnly.FromTimeSpan(TimeSpan.FromMilliseconds(rand.NextInt64(0, 1_000_000)))),
            BuildArray<DateTimeOffset, long, TimestampArray, TimestampArray.Builder>(
                new TimestampArray.Builder(Apache.Arrow.Types.TimeUnit.Millisecond), numRows, random,
                rand => DateTimeOffset.FromUnixTimeMilliseconds(rand.NextInt64(0, DateTimeOffset.MaxValue.ToUnixTimeMilliseconds()))),
            BuildArray<DateTimeOffset, long, TimestampArray, TimestampArray.Builder>(
                new TimestampArray.Builder(Apache.Arrow.Types.TimeUnit.Microsecond), numRows, random,
                rand => DateTimeOffset.FromUnixTimeMilliseconds(rand.NextInt64(0, DateTimeOffset.MaxValue.ToUnixTimeMilliseconds()))),
            BuildArray<Decimal128Array, Decimal128Array.Builder>(
                new Decimal128Array.Builder(new Decimal128Type(29, 3)), numRows, random,
                (builder, rand) => builder.Append(new decimal(rand.NextInt64(0, long.MaxValue) / 1_000))),
            BuildArray<StringArray, StringArray.Builder>(
                new StringArray.Builder(), numRows, random,
                (builder, rand) => builder.Append(RandomString(rand))),
            BuildArray<BinaryArray, BinaryArray.Builder>(
                new BinaryArray.Builder(), numRows, random,
                (builder, rand) => builder.Append(RandomBytes(rand).AsSpan())),
            BuildArray<NullArray, NullArray.Builder>(
                new NullArray.Builder(), numRows, random,
                (builder, _) => builder.AppendNull()),
            BuildDictionaryArray(numRows, random),
            BuildListArray(numRows, random),
            BuildStructArray(numRows, random),
            BuildMapArray(numRows, random),
        };
    }

    private static string RandomString(Random rand)
    {
        return RandomStrings[rand.NextInt64(0, RandomStrings.Length)];
    }

    private static byte[] RandomBytes(Random rand)
    {
        var s = RandomStrings[rand.NextInt64(0, RandomStrings.Length)];
        return System.Text.Encoding.UTF8.GetBytes(s);
    }

    private static readonly string[] RandomStrings = new[]
    {
        "",
        "hello world",
        "abcdefghijklmnopqrstuvwxyz",
        "αβγδεζ",
        "a",
        "b",
        "c",
    };

    private static IArrowArray BuildArray<T, TArray, TBuilder>(TBuilder builder, int numRows, Random random, Func<Random, T> getValue)
        where TArray : IArrowArray
        where TBuilder : IArrowArrayBuilder<T, TArray, TBuilder>
    {
        for (var i = 0; i < numRows; ++i)
        {
            if (random.NextDouble() < 0.1)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append(getValue(random));
            }
        }

        return builder.Build(default);
    }

    private static IArrowArray BuildArray<TArray, TBuilder>(TBuilder builder, int numRows, Random random, Action<TBuilder, Random> appendValue)
        where TArray : IArrowArray
        where TBuilder : IArrowArrayBuilder<TArray, TBuilder>
    {
        for (var i = 0; i < numRows; ++i)
        {
            if (random.NextDouble() < 0.1)
            {
                builder.AppendNull();
            }
            else
            {
                appendValue(builder, random);
            }
        }

        return builder.Build(default);
    }

    private static IArrowArray BuildArray<TFrom, TTo, TArray, TBuilder>(TBuilder builder, int numRows, Random random, Func<Random, TFrom> getValue)
        where TTo : struct
        where TArray : IArrowArray
        where TBuilder : PrimitiveArrayBuilder<TFrom, TTo, TArray, TBuilder>
    {
        for (var i = 0; i < numRows; ++i)
        {
            if (random.NextDouble() < 0.1)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append(getValue(random));
            }
        }

        return builder.Build(default);
    }

    private static IArrowArray BuildDictionaryArray(int numRows, Random random)
    {
        var dictionaryArray = new StringArray.Builder().AppendRange(RandomStrings).Build();
        var indicesArray = BuildArray<byte, UInt8Array, UInt8Array.Builder>(
            new UInt8Array.Builder(), numRows, random, rand => (byte)rand.NextInt64(0, dictionaryArray.Length));
        var dataType = new DictionaryType(indicesArray.Data.DataType, dictionaryArray.Data.DataType, ordered: true);
        return new DictionaryArray(dataType, indicesArray, dictionaryArray);
    }

    private static IArrowArray BuildListArray(int numRows, Random random)
    {
        var builder = new ListArray.Builder(new Int64Type());
        var valueBuilder = (Int64Array.Builder)builder.ValueBuilder;
        for (var rowIdx = 0; rowIdx < numRows; ++rowIdx)
        {
            if (random.NextDouble() < 0.1)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append();
                var listLength = random.NextInt64(0, 10);
                for (var itemIdx = 0; itemIdx < listLength; ++itemIdx)
                {
                    if (random.NextDouble() < 0.1)
                    {
                        valueBuilder.AppendNull();
                    }
                    else
                    {
                        valueBuilder.Append(random.NextInt64(-100, 100));
                    }
                }
            }
        }

        return builder.Build();
    }

    private static IArrowArray BuildStructArray(int numRows, Random random)
    {
        var dataType = new StructType(new Field[]
        {
            new Field("a", new Int32Type(), true),
            new Field("b", new DoubleType(), true),
        });
        var arrayA =
            BuildArray<int, Int32Array, Int32Array.Builder>(
                new Int32Array.Builder(), numRows, random, rand => (int)rand.NextInt64(int.MinValue, 1L + int.MaxValue));
        var arrayB = BuildArray<double, DoubleArray, DoubleArray.Builder>(
            new DoubleArray.Builder(), numRows, random, rand => rand.NextDouble());
        var nullBitmapBuilder = new ArrowBuffer.BitmapBuilder(numRows);
        for (var i = 0; i < numRows; ++i)
        {
            nullBitmapBuilder.Append(random.NextDouble() < 0.05);
        }

        return new StructArray(
            dataType, numRows, new[] { arrayA, arrayB }, nullBitmapBuilder.Build(),
            nullCount: nullBitmapBuilder.UnsetBitCount, offset: 0);
    }

    private static IArrowArray BuildMapArray(int numRows, Random random)
    {
        var dataType = new MapType(new Int32Type(), new FloatType(), nullable: true, keySorted: false);
        var builder = new MapArray.Builder(dataType);
        var keyBuilder = (Int32Array.Builder)builder.KeyBuilder;
        var valueBuilder = (FloatArray.Builder)builder.ValueBuilder;

        for (var rowIdx = 0; rowIdx < numRows; ++rowIdx)
        {
            if (random.NextDouble() < 0.1)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append();
                var mapLength = random.NextInt64(0, 10);
                var keySet = new HashSet<int>();
                for (var keyIdx = 0; keyIdx < mapLength; ++keyIdx)
                {
                    keySet.Add((int)random.NextInt64(0, 100));
                }

                var keys = keySet.ToArray();
                mapLength = keys.Length;
                for (var entryIdx = 0; entryIdx < mapLength; ++entryIdx)
                {
                    keyBuilder.Append(keys[entryIdx]);
                    if (random.NextDouble() < 0.1)
                    {
                        valueBuilder.AppendNull();
                    }
                    else
                    {
                        valueBuilder.Append((float)random.NextDouble());
                    }
                }
            }
        }

        return builder.Build();
    }

    private sealed class MaskedArrayValidator : IArrowArrayVisitor
        , IArrowArrayVisitor<UInt8Array>
        , IArrowArrayVisitor<UInt16Array>
        , IArrowArrayVisitor<UInt32Array>
        , IArrowArrayVisitor<UInt64Array>
        , IArrowArrayVisitor<Int8Array>
        , IArrowArrayVisitor<Int16Array>
        , IArrowArrayVisitor<Int32Array>
        , IArrowArrayVisitor<Int64Array>
        , IArrowArrayVisitor<HalfFloatArray>
        , IArrowArrayVisitor<FloatArray>
        , IArrowArrayVisitor<DoubleArray>
        , IArrowArrayVisitor<BooleanArray>
        , IArrowArrayVisitor<Date32Array>
        , IArrowArrayVisitor<Date64Array>
        , IArrowArrayVisitor<Time32Array>
        , IArrowArrayVisitor<Time64Array>
        , IArrowArrayVisitor<TimestampArray>
        , IArrowArrayVisitor<Decimal128Array>
        , IArrowArrayVisitor<Decimal256Array>
        , IArrowArrayVisitor<StringArray>
        , IArrowArrayVisitor<BinaryArray>
        , IArrowArrayVisitor<NullArray>
        , IArrowArrayVisitor<DictionaryArray>
        , IArrowArrayVisitor<ListArray>
        , IArrowArrayVisitor<StructArray>
    {
        public MaskedArrayValidator(IArrowArray sourceArray, FilterMask? mask)
        {
            _sourceArray = sourceArray;
            _mask = mask;
            _expectedLength = mask?.IncludedCount ?? sourceArray.Length;
        }

        public void Visit(UInt8Array array) => VisitPrimitiveArray<byte, UInt8Array>(array);

        public void Visit(UInt16Array array) => VisitPrimitiveArray<ushort, UInt16Array>(array);

        public void Visit(UInt32Array array) => VisitPrimitiveArray<uint, UInt32Array>(array);

        public void Visit(UInt64Array array) => VisitPrimitiveArray<ulong, UInt64Array>(array);

        public void Visit(Int8Array array) => VisitPrimitiveArray<sbyte, Int8Array>(array);

        public void Visit(Int16Array array) => VisitPrimitiveArray<short, Int16Array>(array);

        public void Visit(Int32Array array) => VisitPrimitiveArray<int, Int32Array>(array);

        public void Visit(Int64Array array) => VisitPrimitiveArray<long, Int64Array>(array);

        public void Visit(HalfFloatArray array) => VisitPrimitiveArray<Half, HalfFloatArray>(array);

        public void Visit(FloatArray array) => VisitPrimitiveArray<float, FloatArray>(array);

        public void Visit(DoubleArray array) => VisitPrimitiveArray<double, DoubleArray>(array);

        public void Visit(BooleanArray array) => VisitArray(array, (arr, arrIdx) => arr.GetValue(arrIdx));

        public void Visit(Date32Array array) => VisitPrimitiveArray<int, Date32Array>(array);

        public void Visit(Date64Array array) => VisitPrimitiveArray<long, Date64Array>(array);

        public void Visit(Time32Array array) => VisitPrimitiveArray<int, Time32Array>(array);

        public void Visit(Time64Array array) => VisitPrimitiveArray<long, Time64Array>(array);

        public void Visit(TimestampArray array) => VisitPrimitiveArray<long, TimestampArray>(array);

        public void Visit(Decimal128Array array) => VisitArray(array, (arr, idx) => arr.GetValue(idx));

        public void Visit(Decimal256Array array) => VisitArray(array, (arr, idx) => arr.GetValue(idx));

        public void Visit(StringArray array) => VisitArray(array, (arr, idx) => arr.GetString(idx));

        public void Visit(BinaryArray array) => VisitArray(array, (arr, idx) => arr.GetBytes(idx).ToArray());

        public void Visit(NullArray array) => VisitArray(array, (_, _) => (object?)null);

        public void Visit(DictionaryArray array)
        {
            if (_sourceArray is not DictionaryArray sourceArray)
            {
                throw new Exception(
                    $"Masked array ({array}) does not have the same type as the source array ({_sourceArray})");
            }

            var dictValidator = new MaskedArrayValidator(sourceArray.Dictionary, mask: null);
            array.Dictionary.Accept(dictValidator);

            var indicesValidator = new MaskedArrayValidator(sourceArray.Indices, _mask);
            array.Indices.Accept(indicesValidator);
        }

        public void Visit(ListArray array)
        {
            if (_sourceArray is not ListArray sourceArray)
            {
                throw new Exception(
                    $"Masked array ({array}) does not have the same type as the source array ({_sourceArray})");
            }

            Assert.That(array.Length, Is.EqualTo(_expectedLength));

            var outputIndex = 0;
            for (var i = 0; i < sourceArray.Length; ++i)
            {
                if (_mask == null || BitUtility.GetBit(_mask.Mask.Span, i))
                {
                    var sourceList = sourceArray.GetSlicedValues(i);
                    var outputList = array.GetSlicedValues(outputIndex);
                    if (sourceList == null)
                    {
                        Assert.That(outputList, Is.Null);
                    }
                    else
                    {
                        var listValidator = new MaskedArrayValidator(sourceList, mask: null);
                        outputList.Accept(listValidator);
                    }

                    outputIndex++;
                }
            }

            Assert.That(outputIndex, Is.EqualTo(_expectedLength));
        }

        public void Visit(StructArray array)
        {
            if (_sourceArray is not StructArray sourceArray)
            {
                throw new Exception(
                    $"Masked array ({array}) does not have the same type as the source array ({_sourceArray})");
            }

            if (array.Fields.Count != sourceArray.Fields.Count)
            {
                throw new Exception($"Struct array field count {array.Fields.Count} does not match expected count ({sourceArray.Fields.Count})");
            }

            Assert.That(array.Length, Is.EqualTo(_expectedLength));

            var outputIndex = 0;
            for (var i = 0; i < sourceArray.Length; ++i)
            {
                if (_mask == null || BitUtility.GetBit(_mask.Mask.Span, i))
                {
                    Assert.That(array.IsValid(outputIndex), Is.EqualTo(sourceArray.IsValid(i)));
                    outputIndex++;
                }
            }

            for (var fieldIdx = 0; fieldIdx < sourceArray.Fields.Count; ++fieldIdx)
            {
                // StructArray.Fields doesn't account for offset and length, so we need to slice here,
                // see https://github.com/apache/arrow/issues/40790
                var slicedSource = ArrowArrayFactory.Slice(sourceArray.Fields[fieldIdx], sourceArray.Offset, sourceArray.Length);
                var slicedField = ArrowArrayFactory.Slice(array.Fields[fieldIdx], array.Offset, array.Length);
                var fieldValidator = new MaskedArrayValidator(slicedSource, _mask);
                slicedField.Accept(fieldValidator);
            }
        }

        public void Visit(IArrowArray array)
        {
            throw new NotImplementedException($"Masked array validation not implemented for type {array.Data.DataType}");
        }

        private void VisitPrimitiveArray<T, TArray>(TArray array)
            where T : struct, IEquatable<T>
            where TArray : PrimitiveArray<T>
        {
            VisitArray(array, (arr, arrIdx) => arr.GetValue(arrIdx));
        }

        private void VisitArray<T, TArray>(TArray array, Func<TArray, int, T> getter)
            where TArray : IArrowArray
        {
            if (_sourceArray is not TArray sourceArray)
            {
                throw new Exception(
                    $"Masked array ({array}) does not have the same type as the source array ({_sourceArray})");
            }

            Assert.That(array.Length, Is.EqualTo(_expectedLength));

            var outputIndex = 0;
            for (var i = 0; i < sourceArray.Length; ++i)
            {
                if (_mask == null || BitUtility.GetBit(_mask.Mask.Span, i))
                {
                    var sourceValue = getter(sourceArray, i);
                    var outputValue = getter(array, outputIndex);
                    Assert.That(outputValue, Is.EqualTo(sourceValue),
                        $"Expected masked array value at index {outputIndex} to match source value at index {i}");
                    outputIndex++;
                }
            }

            Assert.That(outputIndex, Is.EqualTo(_expectedLength));
        }

        private readonly IArrowArray _sourceArray;
        private readonly FilterMask? _mask;
        private readonly int _expectedLength;
    }
}
