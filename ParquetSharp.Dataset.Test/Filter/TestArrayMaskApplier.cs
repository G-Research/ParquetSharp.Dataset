using System;
using Apache.Arrow;
using NUnit.Framework;
using ParquetSharp.Dataset.Filter;

namespace ParquetSharp.Dataset.Test.Filter;

[TestFixture]
public class TestArrayMaskApplier
{
    [Test]
    public void TestFilterArrays()
    {
        var random = new Random(0);
        const int numRows = 1_001;

        var arrays = new IArrowArray[]
        {
            BuildArray<byte, UInt8Array, UInt8Array.Builder>(numRows, random, rand => (byte)rand.NextInt64(0, byte.MaxValue)),
            BuildArray<ushort, UInt16Array, UInt16Array.Builder>(numRows, random, rand => (ushort)rand.NextInt64(0, ushort.MaxValue)),
            BuildArray<uint, UInt32Array, UInt32Array.Builder>(numRows, random, rand => (uint)rand.NextInt64(0, uint.MaxValue)),
            BuildArray<ulong, UInt64Array, UInt64Array.Builder>(numRows, random, rand => unchecked((ulong)rand.NextInt64(long.MinValue, long.MaxValue))),
            BuildArray<sbyte, Int8Array, Int8Array.Builder>(numRows, random, rand => (sbyte)rand.NextInt64(sbyte.MinValue, sbyte.MaxValue)),
            BuildArray<short, Int16Array, Int16Array.Builder>(numRows, random, rand => (short)rand.NextInt64(short.MinValue, short.MaxValue)),
            BuildArray<int, Int32Array, Int32Array.Builder>(numRows, random, rand => (int)rand.NextInt64(int.MinValue, int.MaxValue)),
            BuildArray<long, Int64Array, Int64Array.Builder>(numRows, random, rand => rand.NextInt64(long.MinValue, long.MaxValue)),
            BuildArray<Half, HalfFloatArray, HalfFloatArray.Builder>(numRows, random, rand => (Half)rand.NextDouble()),
            BuildArray<float, FloatArray, FloatArray.Builder>(numRows, random, rand => (float)rand.NextDouble()),
            BuildArray<double, DoubleArray, DoubleArray.Builder>(numRows, random, rand => rand.NextDouble()),
        };

        var bitMask = new byte[BitUtility.ByteCount(numRows)];
        for (var i = 0; i < numRows; ++i)
        {
            var included = random.NextDouble() < 0.5;
            BitUtility.SetBit(bitMask, i, included);
        }

        var mask = new FilterMask(bitMask);

        foreach (var array in arrays)
        {
            var applier = new ArrayMaskApplier(mask);
            array.Accept(applier);
            var masked = applier.MaskedArray;

            Assert.That(masked.Length, Is.EqualTo(mask.IncludedCount));
            var validator = new MaskedArrayValidator(array, mask);
            masked.Accept(validator);
        }
    }

    private static IArrowArray BuildArray<T, TArray, TBuilder>(int numRows, Random random, Func<Random, T> getValue)
        where TArray : IArrowArray
        where TBuilder : IArrowArrayBuilder<T, TArray, TBuilder>, new()
    {
        var builder = new TBuilder();
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
    {
        public MaskedArrayValidator(IArrowArray sourceArray, FilterMask mask)
        {
            _sourceArray = sourceArray;
            _mask = mask;
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

        public void Visit(IArrowArray array)
        {
            throw new NotImplementedException($"Masked array validation not implemented for type {array.Data.DataType}");
        }

        private void VisitPrimitiveArray<T, TArray>(TArray array)
            where T : struct
            where TArray : PrimitiveArray<T>
        {
            if (_sourceArray is not TArray sourceArray)
            {
                throw new Exception(
                    $"Masked array ({array}) does not have the same type as the source array ({_sourceArray})");
            }

            var outputIndex = 0;
            for (var i = 0; i < _sourceArray.Length; ++i)
            {
                if (BitUtility.GetBit(_mask.Mask.Span, i))
                {
                    Assert.That(array.GetValue(outputIndex), Is.EqualTo(sourceArray.GetValue(i)));
                    outputIndex++;
                }
            }

            Assert.That(outputIndex, Is.EqualTo(_mask.IncludedCount));
        }

        private readonly IArrowArray _sourceArray;
        private readonly FilterMask _mask;
    }
}
