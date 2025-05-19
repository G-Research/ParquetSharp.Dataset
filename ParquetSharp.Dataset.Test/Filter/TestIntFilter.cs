using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;
using NUnit.Framework;
using ParquetSharp.Dataset.Filter;

namespace ParquetSharp.Dataset.Test.Filter;

[TestFixture]
public class TestIntFilter
{
    [DatapointSource]
    private static readonly long[] LongValues =
    {
        long.MinValue, long.MinValue + 1L,
        int.MinValue - 1L, int.MinValue, int.MinValue + 1L,
        short.MinValue - 1L, short.MinValue, short.MinValue + 1L,
        sbyte.MinValue - 1L, sbyte.MinValue, sbyte.MinValue + 1L,
        -3, -2, -1, 0, 1, 2, 3,
        sbyte.MaxValue - 1L, sbyte.MaxValue, sbyte.MaxValue + 1L,
        byte.MaxValue - 1L, byte.MaxValue, byte.MaxValue + 1L,
        short.MaxValue - 1L, short.MaxValue, short.MaxValue + 1L,
        ushort.MaxValue - 1L, ushort.MaxValue, ushort.MaxValue + 1L,
        int.MaxValue - 1L, int.MaxValue, int.MaxValue + 1L,
        uint.MaxValue - 1L, uint.MaxValue, uint.MaxValue + 1L,
        long.MaxValue - 1L, long.MaxValue,
    };

    [DatapointSource]
    private static readonly (long, long)[] FilterRanges = LongValues
        .SelectMany(rangeStart => LongValues.Select(rangeEnd => (rangeStart, rangeEnd)))
        .Where(range => range.rangeEnd >= range.rangeStart)
        .Concat(new (long, long)[] { (2, 1) })
        .ToArray();

    private static readonly int[] IntValues =
    {
        int.MinValue, int.MinValue + 1,
        short.MinValue - 1, short.MinValue, short.MinValue + 1,
        sbyte.MinValue - 1, sbyte.MinValue, sbyte.MinValue + 1,
        -3, -2, -1, 0, 1, 2, 3,
        sbyte.MaxValue - 1, sbyte.MaxValue, sbyte.MaxValue + 1,
        byte.MaxValue - 1, byte.MaxValue, byte.MaxValue + 1,
        short.MaxValue - 1, short.MaxValue, short.MaxValue + 1,
        ushort.MaxValue - 1, ushort.MaxValue, ushort.MaxValue + 1,
        int.MaxValue - 1, int.MaxValue,
    };

    private static readonly short[] ShortValues =
    {
        short.MinValue, short.MinValue + 1,
        sbyte.MinValue - 1, sbyte.MinValue, sbyte.MinValue + 1,
        -3, -2, -1, 0, 1, 2, 3,
        sbyte.MaxValue - 1, sbyte.MaxValue, sbyte.MaxValue + 1,
        byte.MaxValue - 1, byte.MaxValue, byte.MaxValue + 1,
        short.MaxValue - 1, short.MaxValue,
    };

    private static readonly sbyte[] SByteValues =
    {
        sbyte.MinValue, sbyte.MinValue + 1,
        -3, -2, -1, 0, 1, 2, 3,
        sbyte.MaxValue - 1, sbyte.MaxValue,
    };

    private static readonly ulong[] ULongValues =
    {
        0, 1, 2, 3,
        (ulong)sbyte.MaxValue - 1UL, (ulong)sbyte.MaxValue, (ulong)sbyte.MaxValue + 1UL,
        byte.MaxValue - 1UL, byte.MaxValue, byte.MaxValue + 1UL,
        (ulong)short.MaxValue - 1UL, (ulong)short.MaxValue, (ulong)short.MaxValue + 1UL,
        ushort.MaxValue - 1UL, ushort.MaxValue, ushort.MaxValue + 1UL,
        int.MaxValue - 1UL, int.MaxValue, int.MaxValue + 1UL,
        uint.MaxValue - 1UL, uint.MaxValue, uint.MaxValue + 1UL,
        long.MaxValue - 1UL, long.MaxValue, long.MaxValue + 1UL,
        ulong.MaxValue - 1UL, ulong.MaxValue,
    };

    private static readonly uint[] UIntValues =
    {
        0, 1, 2, 3,
        (uint)sbyte.MaxValue, sbyte.MaxValue + 1,
        byte.MaxValue - 1, byte.MaxValue, byte.MaxValue + 1,
        short.MaxValue - 1, (uint)short.MaxValue, short.MaxValue + 1,
        ushort.MaxValue - 1, ushort.MaxValue, ushort.MaxValue + 1,
        int.MaxValue - 1, int.MaxValue, int.MaxValue + 1u,
        uint.MaxValue - 1, uint.MaxValue,
    };

    private static readonly ushort[] UShortValues =
    {
        0, 1, 2, 3,
        sbyte.MaxValue - 1, (ushort)sbyte.MaxValue, (ushort)sbyte.MaxValue + 1,
        byte.MaxValue - 1, byte.MaxValue, byte.MaxValue + 1,
        short.MaxValue - 1, (ushort)short.MaxValue,
        ushort.MaxValue - 1, ushort.MaxValue,
    };

    private static readonly byte[] ByteValues =
    {
        0, 1, 2, 3,
        byte.MaxValue - 1, byte.MaxValue
    };

    [Theory]
    public void TestComputeIntEqualityMask(long filterValue)
    {
        var filter = Col.Named("x").IsEqualTo(filterValue);
        TestComputeIntComparisonMask<sbyte, Int8Array, Int8Array.Builder>(filter, SByteValues, val => val == filterValue);
        TestComputeIntComparisonMask<short, Int16Array, Int16Array.Builder>(filter, ShortValues, val => val == filterValue);
        TestComputeIntComparisonMask<int, Int32Array, Int32Array.Builder>(filter, IntValues, val => val == filterValue);
        TestComputeIntComparisonMask<long, Int64Array, Int64Array.Builder>(filter, LongValues, val => val == filterValue);
        TestComputeIntComparisonMask<byte, UInt8Array, UInt8Array.Builder>(filter, ByteValues, val => val == filterValue);
        TestComputeIntComparisonMask<ushort, UInt16Array, UInt16Array.Builder>(filter, UShortValues, val => val == filterValue);
        TestComputeIntComparisonMask<uint, UInt32Array, UInt32Array.Builder>(filter, UIntValues, val => val == filterValue);
        TestComputeIntComparisonMask<ulong, UInt64Array, UInt64Array.Builder>(filter, ULongValues, val => filterValue >= 0 && val == (ulong)filterValue);
    }

    [Theory]
    public void TestComputeIntGtMask(long filterValue)
    {
        var filter = Col.Named("x").IsGreaterThan(filterValue);
        TestComputeIntComparisonMask<sbyte, Int8Array, Int8Array.Builder>(filter, SByteValues, val => val > filterValue);
        TestComputeIntComparisonMask<short, Int16Array, Int16Array.Builder>(filter, ShortValues, val => val > filterValue);
        TestComputeIntComparisonMask<int, Int32Array, Int32Array.Builder>(filter, IntValues, val => val > filterValue);
        TestComputeIntComparisonMask<long, Int64Array, Int64Array.Builder>(filter, LongValues, val => val > filterValue);
        TestComputeIntComparisonMask<byte, UInt8Array, UInt8Array.Builder>(filter, ByteValues, val => val > filterValue);
        TestComputeIntComparisonMask<ushort, UInt16Array, UInt16Array.Builder>(filter, UShortValues, val => val > filterValue);
        TestComputeIntComparisonMask<uint, UInt32Array, UInt32Array.Builder>(filter, UIntValues, val => val > filterValue);
        TestComputeIntComparisonMask<ulong, UInt64Array, UInt64Array.Builder>(filter, ULongValues, val => filterValue < 0 || val > (ulong)filterValue);
    }

    [Theory]
    public void TestComputeIntGtEqMask(long filterValue)
    {
        var filter = Col.Named("x").IsGreaterThanOrEqual(filterValue);
        TestComputeIntComparisonMask<sbyte, Int8Array, Int8Array.Builder>(filter, SByteValues, val => val >= filterValue);
        TestComputeIntComparisonMask<short, Int16Array, Int16Array.Builder>(filter, ShortValues, val => val >= filterValue);
        TestComputeIntComparisonMask<int, Int32Array, Int32Array.Builder>(filter, IntValues, val => val >= filterValue);
        TestComputeIntComparisonMask<long, Int64Array, Int64Array.Builder>(filter, LongValues, val => val >= filterValue);
        TestComputeIntComparisonMask<byte, UInt8Array, UInt8Array.Builder>(filter, ByteValues, val => val >= filterValue);
        TestComputeIntComparisonMask<ushort, UInt16Array, UInt16Array.Builder>(filter, UShortValues, val => val >= filterValue);
        TestComputeIntComparisonMask<uint, UInt32Array, UInt32Array.Builder>(filter, UIntValues, val => val >= filterValue);
        TestComputeIntComparisonMask<ulong, UInt64Array, UInt64Array.Builder>(filter, ULongValues, val => filterValue < 0 || val >= (ulong)filterValue);
    }

    [Theory]
    public void TestComputeIntLtMask(long filterValue)
    {
        var filter = Col.Named("x").IsLessThan(filterValue);
        TestComputeIntComparisonMask<sbyte, Int8Array, Int8Array.Builder>(filter, SByteValues, val => val < filterValue);
        TestComputeIntComparisonMask<short, Int16Array, Int16Array.Builder>(filter, ShortValues, val => val < filterValue);
        TestComputeIntComparisonMask<int, Int32Array, Int32Array.Builder>(filter, IntValues, val => val < filterValue);
        TestComputeIntComparisonMask<long, Int64Array, Int64Array.Builder>(filter, LongValues, val => val < filterValue);
        TestComputeIntComparisonMask<byte, UInt8Array, UInt8Array.Builder>(filter, ByteValues, val => val < filterValue);
        TestComputeIntComparisonMask<ushort, UInt16Array, UInt16Array.Builder>(filter, UShortValues, val => val < filterValue);
        TestComputeIntComparisonMask<uint, UInt32Array, UInt32Array.Builder>(filter, UIntValues, val => val < filterValue);
        TestComputeIntComparisonMask<ulong, UInt64Array, UInt64Array.Builder>(filter, ULongValues, val => filterValue >= 0 && val < (ulong)filterValue);
    }

    [Theory]
    public void TestComputeIntLtEqMask(long filterValue)
    {
        var filter = Col.Named("x").IsLessThanOrEqual(filterValue);
        TestComputeIntComparisonMask<sbyte, Int8Array, Int8Array.Builder>(filter, SByteValues, val => val <= filterValue);
        TestComputeIntComparisonMask<short, Int16Array, Int16Array.Builder>(filter, ShortValues, val => val <= filterValue);
        TestComputeIntComparisonMask<int, Int32Array, Int32Array.Builder>(filter, IntValues, val => val <= filterValue);
        TestComputeIntComparisonMask<long, Int64Array, Int64Array.Builder>(filter, LongValues, val => val <= filterValue);
        TestComputeIntComparisonMask<byte, UInt8Array, UInt8Array.Builder>(filter, ByteValues, val => val <= filterValue);
        TestComputeIntComparisonMask<ushort, UInt16Array, UInt16Array.Builder>(filter, UShortValues, val => val <= filterValue);
        TestComputeIntComparisonMask<uint, UInt32Array, UInt32Array.Builder>(filter, UIntValues, val => val <= filterValue);
        TestComputeIntComparisonMask<ulong, UInt64Array, UInt64Array.Builder>(filter, ULongValues, val => filterValue >= 0 && val <= (ulong)filterValue);
    }

    [Theory]
    public void TestComputeIntRangeMask((long, long) filterRange)
    {
        var (rangeStart, rangeEnd) = filterRange;
        TestComputeIntRangeMask<sbyte, Int8Array, Int8Array.Builder>(rangeStart, rangeEnd, SByteValues, val => val);
        TestComputeIntRangeMask<short, Int16Array, Int16Array.Builder>(rangeStart, rangeEnd, ShortValues, val => val);
        TestComputeIntRangeMask<int, Int32Array, Int32Array.Builder>(rangeStart, rangeEnd, IntValues, val => val);
        TestComputeIntRangeMask<long, Int64Array, Int64Array.Builder>(rangeStart, rangeEnd, LongValues, val => val);
        TestComputeIntRangeMask<byte, UInt8Array, UInt8Array.Builder>(rangeStart, rangeEnd, ByteValues, val => val);
        TestComputeIntRangeMask<ushort, UInt16Array, UInt16Array.Builder>(rangeStart, rangeEnd, UShortValues, val => val);
        TestComputeIntRangeMask<uint, UInt32Array, UInt32Array.Builder>(rangeStart, rangeEnd, UIntValues, val => val);
        TestComputeIntRangeMask<ulong, UInt64Array, UInt64Array.Builder>(rangeStart, rangeEnd, ULongValues, val => checked((long)val));
    }

    [Theory]
    public void TestIntEqualityIncludeRowGroup(long filterValue)
    {
        var filter = Col.Named("x").IsEqualTo(filterValue);
        TestIntComparisonIncludeRowGroup(filter, SByteValues, (min, max) => filterValue >= min && filterValue <= max);
        TestIntComparisonIncludeRowGroup(filter, ShortValues, (min, max) => filterValue >= min && filterValue <= max);
        TestIntComparisonIncludeRowGroup(filter, IntValues, (min, max) => filterValue >= min && filterValue <= max);
        TestIntComparisonIncludeRowGroup(filter, LongValues, (min, max) => filterValue >= min && filterValue <= max);
        TestIntComparisonIncludeRowGroup(filter, ByteValues, (min, max) => filterValue >= min && filterValue <= max);
        TestIntComparisonIncludeRowGroup(filter, UShortValues, (min, max) => filterValue >= min && filterValue <= max);
        TestIntComparisonIncludeRowGroup(filter, UIntValues, (min, max) => filterValue >= min && filterValue <= max);
        TestIntComparisonIncludeRowGroup(filter, ULongValues, (min, max) => filterValue >= 0 && (ulong)filterValue >= min && (ulong)filterValue <= max);
    }

    [Theory]
    public void TestIntGtIncludeRowGroup(long filterValue)
    {
        var filter = Col.Named("x").IsGreaterThan(filterValue);
        TestIntComparisonIncludeRowGroup(filter, SByteValues, (_, max) => filterValue < max);
        TestIntComparisonIncludeRowGroup(filter, ShortValues, (_, max) => filterValue < max);
        TestIntComparisonIncludeRowGroup(filter, IntValues, (_, max) => filterValue < max);
        TestIntComparisonIncludeRowGroup(filter, LongValues, (_, max) => filterValue < max);
        TestIntComparisonIncludeRowGroup(filter, ByteValues, (_, max) => filterValue < max);
        TestIntComparisonIncludeRowGroup(filter, UShortValues, (_, max) => filterValue < max);
        TestIntComparisonIncludeRowGroup(filter, UIntValues, (_, max) => filterValue < max);
        TestIntComparisonIncludeRowGroup(filter, ULongValues, (_, max) => filterValue < 0 || (ulong)filterValue < max);
    }

    [Theory]
    public void TestIntGtEqIncludeRowGroup(long filterValue)
    {
        var filter = Col.Named("x").IsGreaterThanOrEqual(filterValue);
        TestIntComparisonIncludeRowGroup(filter, SByteValues, (_, max) => filterValue <= max);
        TestIntComparisonIncludeRowGroup(filter, ShortValues, (_, max) => filterValue <= max);
        TestIntComparisonIncludeRowGroup(filter, IntValues, (_, max) => filterValue <= max);
        TestIntComparisonIncludeRowGroup(filter, LongValues, (_, max) => filterValue <= max);
        TestIntComparisonIncludeRowGroup(filter, ByteValues, (_, max) => filterValue <= max);
        TestIntComparisonIncludeRowGroup(filter, UShortValues, (_, max) => filterValue <= max);
        TestIntComparisonIncludeRowGroup(filter, UIntValues, (_, max) => filterValue <= max);
        TestIntComparisonIncludeRowGroup(filter, ULongValues, (_, max) => filterValue < 0 || (ulong)filterValue <= max);
    }

    [Theory]
    public void TestIntLtIncludeRowGroup(long filterValue)
    {
        var filter = Col.Named("x").IsLessThan(filterValue);
        TestIntComparisonIncludeRowGroup(filter, SByteValues, (min, _) => filterValue > min);
        TestIntComparisonIncludeRowGroup(filter, ShortValues, (min, _) => filterValue > min);
        TestIntComparisonIncludeRowGroup(filter, IntValues, (min, _) => filterValue > min);
        TestIntComparisonIncludeRowGroup(filter, LongValues, (min, _) => filterValue > min);
        TestIntComparisonIncludeRowGroup(filter, ByteValues, (min, _) => filterValue > min);
        TestIntComparisonIncludeRowGroup(filter, UShortValues, (min, _) => filterValue > min);
        TestIntComparisonIncludeRowGroup(filter, UIntValues, (min, _) => filterValue > min);
        TestIntComparisonIncludeRowGroup(filter, ULongValues, (min, _) => filterValue >= 0 && (ulong)filterValue > min);
    }

    [Theory]
    public void TestIntLtEqIncludeRowGroup(long filterValue)
    {
        var filter = Col.Named("x").IsLessThanOrEqual(filterValue);
        TestIntComparisonIncludeRowGroup(filter, SByteValues, (min, _) => filterValue >= min);
        TestIntComparisonIncludeRowGroup(filter, ShortValues, (min, _) => filterValue >= min);
        TestIntComparisonIncludeRowGroup(filter, IntValues, (min, _) => filterValue >= min);
        TestIntComparisonIncludeRowGroup(filter, LongValues, (min, _) => filterValue >= min);
        TestIntComparisonIncludeRowGroup(filter, ByteValues, (min, _) => filterValue >= min);
        TestIntComparisonIncludeRowGroup(filter, UShortValues, (min, _) => filterValue >= min);
        TestIntComparisonIncludeRowGroup(filter, UIntValues, (min, _) => filterValue >= min);
        TestIntComparisonIncludeRowGroup(filter, ULongValues, (min, _) => filterValue >= 0 && (ulong)filterValue >= min);
    }

    [Theory]
    public void TestIntRangeIncludeRowGroup((long, long) filterRange)
    {
        var (rangeStart, rangeEnd) = filterRange;
        TestIntRangeIncludeRowGroup(rangeStart, rangeEnd, SByteValues, val => val);
        TestIntRangeIncludeRowGroup(rangeStart, rangeEnd, ShortValues, val => val);
        TestIntRangeIncludeRowGroup(rangeStart, rangeEnd, IntValues, val => val);
        TestIntRangeIncludeRowGroup(rangeStart, rangeEnd, LongValues, val => val);
        TestIntRangeIncludeRowGroup(rangeStart, rangeEnd, ByteValues, val => val);
        TestIntRangeIncludeRowGroup(rangeStart, rangeEnd, UShortValues, val => val);
        TestIntRangeIncludeRowGroup(rangeStart, rangeEnd, UIntValues, val => val);
        TestIntRangeIncludeRowGroup(rangeStart, rangeEnd, ULongValues, val => checked((long)val));
    }

    [Test]
    public void TestImpossibleFilter()
    {
        var builder = new Int32Array.Builder();
        builder.AppendRange(new[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
        var array = builder.Build();
        var recordBatch = new RecordBatch.Builder().Append("x", true, array).Build();

        var filter = Col.Named("x").IsGreaterThan((long)Int32.MaxValue + 1);
        var mask = filter.ComputeMask(recordBatch);

        Assert.That(mask, Is.Not.Null);
        Assert.That(mask.Mask.Length, Is.EqualTo(2));
        Assert.That(mask.IncludedCount, Is.EqualTo(0));
    }

    [Test]
    public void TestRedundantFilterWithNulls()
    {
        var builder = new Int32Array.Builder();
        builder.AppendNull();
        builder.AppendRange(new[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
        var array = builder.Build();
        var recordBatch = new RecordBatch.Builder().Append("x", true, array).Build();

        var filter = Col.Named("x").IsGreaterThan((long)Int32.MinValue - 1);
        var mask = filter.ComputeMask(recordBatch);

        Assert.That(mask, Is.Not.Null);
        Assert.That(mask.Mask.Length, Is.EqualTo(2));
        Assert.That(mask.IncludedCount, Is.EqualTo(10));
    }

    [Test]
    public void TestRedundantFilterWithoutNulls()
    {
        var builder = new Int32Array.Builder();
        builder.AppendRange(new[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
        var array = builder.Build();
        var recordBatch = new RecordBatch.Builder().Append("x", false, array).Build();

        var filter = Col.Named("x").IsGreaterThan((long)Int32.MinValue - 1);
        var mask = filter.ComputeMask(recordBatch);

        Assert.That(mask, Is.Not.Null);
        Assert.That(mask.Mask.Length, Is.EqualTo(2));
        Assert.That(mask.IncludedCount, Is.EqualTo(10));
    }

    private static void TestComputeIntComparisonMask<T, TArray, TBuilder>(IFilter filter, T[] values, Func<T, bool> comparison)
        where T : struct, IEquatable<T>
        where TArray : PrimitiveArray<T>
        where TBuilder : PrimitiveArrayBuilder<T, TArray, TBuilder>, new()
    {
        var array = BuildArray<T, TArray, TBuilder>(values);
        var recordBatch = new RecordBatch.Builder().Append("x", true, array).Build();

        var mask = filter.ComputeMask(recordBatch);

        Assert.That(mask, Is.Not.Null);
        Assert.That(mask!.Mask.Length, Is.EqualTo(BitUtility.ByteCount(array.Length)));

        for (var i = 0; i < array.Length; ++i)
        {
            var isIncluded = BitUtility.GetBit(mask.Mask.Span, i);
            var arrayValue = array.GetValue(i);
            var comparisonIsTrue = false;
            if (arrayValue.HasValue)
            {
                comparisonIsTrue = comparison(arrayValue.Value);
            }

            var valueString = arrayValue.HasValue ? arrayValue.Value.ToString() : "null";
            Assert.That(
                isIncluded, Is.EqualTo(comparisonIsTrue),
                $"Expected {typeof(T)} value {valueString} inclusion to be {comparisonIsTrue}");
        }
    }

    private static void TestComputeIntRangeMask<T, TArray, TBuilder>(long rangeStart, long rangeEnd, T[] values, Func<T, long> checkedCast)
        where T : struct, IEquatable<T>
        where TArray : PrimitiveArray<T>
        where TBuilder : PrimitiveArrayBuilder<T, TArray, TBuilder>, new()
    {
        var filter = Col.Named("x").IsInRange(rangeStart, rangeEnd);

        var array = BuildArray<T, TArray, TBuilder>(values);
        var recordBatch = new RecordBatch.Builder().Append("x", true, array).Build();

        var mask = filter.ComputeMask(recordBatch);

        Assert.That(mask, Is.Not.Null);
        Assert.That(mask!.Mask.Length, Is.EqualTo(BitUtility.ByteCount(array.Length)));

        for (var i = 0; i < array.Length; ++i)
        {
            var isIncluded = BitUtility.GetBit(mask.Mask.Span, i);
            var arrayValue = array.GetValue(i);
            var isInRange = false;
            if (arrayValue.HasValue)
            {
                try
                {
                    var asLong = checkedCast(arrayValue.Value);
                    isInRange = asLong >= rangeStart && asLong < rangeEnd;
                }
                catch (OverflowException)
                {
                }
            }

            Assert.That(
                isIncluded, Is.EqualTo(isInRange),
                $"Expected {typeof(T)} value {arrayValue} inclusion to be {isInRange} for range [{rangeStart}, {rangeEnd})");
        }
    }

    private static void TestIntComparisonIncludeRowGroup<T>(IFilter filter, T[] values, Func<T, T, bool> possibleMatch)
        where T : IComparable<T>
    {
        var statsRanges = values
            .SelectMany(min => values.Select(max => (min, max)))
            .Where(range => range.max.CompareTo(range.min) >= 0)
            .ToArray();
        foreach (var statsRange in statsRanges)
        {
            var rowGroupStats = new Dictionary<string, LogicalStatistics>
            {
                { "x", new LogicalStatistics<T>(statsRange.min, statsRange.max) }
            };

            var expected = possibleMatch(statsRange.min, statsRange.max);
            var includeRowGroup = filter.IncludeRowGroup(rowGroupStats);

            Assert.That(
                includeRowGroup, Is.EqualTo(expected),
                $"Expected {typeof(T)} stats range [{statsRange.min}, {statsRange.max}] inclusion to be {expected}");
        }
    }

    private static void TestIntRangeIncludeRowGroup<T>(long rangeStart, long rangeEnd, T[] values, Func<T, long> checkedCast)
        where T : IComparable<T>
    {
        var filter = Col.Named("x").IsInRange(rangeStart, rangeEnd);

        var statsRanges = values
            .SelectMany(min => values.Select(max => (min, max)))
            .Where(range => range.max.CompareTo(range.min) >= 0)
            .ToArray();
        foreach (var statsRange in statsRanges)
        {
            var rowGroupStats = new Dictionary<string, LogicalStatistics>
            {
                { "x", new LogicalStatistics<T>(statsRange.min, statsRange.max) }
            };

            var rangesOverlap = true;
            try
            {
                var longMin = checkedCast(statsRange.min);
                if (longMin >= rangeEnd)
                {
                    rangesOverlap = false;
                }
            }
            catch (OverflowException)
            {
                rangesOverlap = false;
            }

            try
            {
                var longMax = checkedCast(statsRange.max);
                if (longMax < rangeStart)
                {
                    rangesOverlap = false;
                }
            }
            catch (OverflowException)
            {
            }

            var includeRowGroup = filter.IncludeRowGroup(rowGroupStats);
            Assert.That(
                includeRowGroup, Is.EqualTo(rangesOverlap),
                $"Expected {typeof(T)} stats range [{statsRange.min}, {statsRange.max}] inclusion to be {rangesOverlap}");
        }
    }

    private static TArray BuildArray<T, TArray, TBuilder>(T[] values)
        where T : struct
        where TArray : IArrowArray
        where TBuilder : PrimitiveArrayBuilder<T, TArray, TBuilder>, new()
    {
        var builder = new TBuilder();
        foreach (var value in values)
        {
            builder.AppendNull();
            builder.Append(value);
        }

        return builder.Build();
    }
}
