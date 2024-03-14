using Apache.Arrow;
using NUnit.Framework;

namespace ParquetSharp.Dataset.Test;

[TestFixture]
public class TestFilter
{
    [Test]
    public void TestFieldNotInPartitionInfo()
    {
        var filter = Col.Named("x").IsEqualTo(5).And(Col.Named("y").IsEqualTo("abc"));

        var partitionInfo = new PartitionInformation(
            new RecordBatch.Builder()
                .Append("z", nullable: true, new Int64Array.Builder().Append(1))
                .Build());

        Assert.That(filter.IncludePartition(partitionInfo));
    }

    [Test]
    public void TestStringEqualityFilter()
    {
        var filter = Col.Named("x").IsEqualTo("abc");

        var partitionInfo = new PartitionInformation(
            new RecordBatch.Builder()
                .Append("x", nullable: true, new StringArray.Builder().Append("abc"))
                .Build());

        Assert.That(filter.IncludePartition(partitionInfo));

        partitionInfo = new PartitionInformation(
            new RecordBatch.Builder()
                .Append("x", nullable: true, new StringArray.Builder().Append("def"))
                .Build());

        Assert.That(filter.IncludePartition(partitionInfo), Is.False);
    }

    [Test]
    public void TestStringSetFilter()
    {
        var filter = Col.Named("x").IsIn(new [] {"abc", "def"});

        var partitionInfo = new PartitionInformation(
            new RecordBatch.Builder()
                .Append("x", nullable: true, new StringArray.Builder().Append("def"))
                .Build());

        Assert.That(filter.IncludePartition(partitionInfo));

        partitionInfo = new PartitionInformation(
            new RecordBatch.Builder()
                .Append("x", nullable: true, new StringArray.Builder().Append("ghi"))
                .Build());

        Assert.That(filter.IncludePartition(partitionInfo), Is.False);
    }

    [Test]
    public void TestIntValueFilter()
    {
        var filter = Col.Named("x").IsEqualTo(2);

        var negativeValueFilter = Col.Named("x").IsEqualTo(-2);

        var maxInt64Filter = Col.Named("x").IsEqualTo(long.MaxValue);

        foreach (var (value, expected) in new (long?, bool)[]
                 {
                     (2, true),
                     (1, false),
                     (null, false),
                 })
        {
            TestIntColumnFilterWithAllTypes(filter, value, expected);
        }

        TestIntColumnFilterWithAllTypes(negativeValueFilter, 2, false);
        TestIntColumnFilterWithAllTypes(maxInt64Filter, 2, false);

        TestUInt64ColumnFilter(maxInt64Filter, long.MaxValue, true);
        TestUInt64ColumnFilter(maxInt64Filter, ulong.MaxValue, false);
        TestInt8ColumnFilter(negativeValueFilter, -2, true);
        TestInt16ColumnFilter(negativeValueFilter, -2, true);
        TestInt32ColumnFilter(negativeValueFilter, -2, true);
        TestInt64ColumnFilter(negativeValueFilter, -2, true);
        TestInt64ColumnFilter(maxInt64Filter, long.MaxValue, true);
    }

    [Test]
    public void TestIntRangeFilter()
    {
        var filter = Col.Named("x").IsInRange(2, 5);

        foreach (var (value, expected) in new[]
                 {
                     (1, false),
                     (2, true),
                     (3, true),
                     (5, true),
                     (6, false),
                 })
        {
            TestIntColumnFilterWithAllTypes(filter, value, expected);
        }
    }

    [Test]
    public void TestIntRangeFilterCrossingZero()
    {
        var filter = Col.Named("x").IsInRange(-2, 5);

        foreach (var (value, expected) in new[]
                 {
                     (0, true),
                     (1, true),
                     (5, true),
                     (6, false),
                 })
        {
            TestIntColumnFilterWithAllTypes(filter, value, expected);
        }

        foreach (var (value, expected) in new[]
                 {
                     (-3, false),
                     (-2, true),
                     (-1, true),
                 })
        {
            TestInt8ColumnFilter(filter, (sbyte) value, expected);
            TestInt16ColumnFilter(filter, (short) value, expected);
            TestInt32ColumnFilter(filter, (int) value, expected);
            TestInt64ColumnFilter(filter, (long) value, expected);
        }
    }

    [Test]
    public void TestNegativeIntRangeFilter()
    {
        var filter = Col.Named("x").IsInRange(-5, -2);

        foreach (var value in new [] {0, 1})
        {
            TestIntColumnFilterWithAllTypes(filter, value, false);
        }

        foreach (var (value, expected) in new[]
                 {
                     (-6, false),
                     (-5, true),
                     (-2, true),
                     (-1, false),
                 })
        {
            TestInt8ColumnFilter(filter, (sbyte) value, expected);
            TestInt16ColumnFilter(filter, (short) value, expected);
            TestInt32ColumnFilter(filter, (int) value, expected);
            TestInt64ColumnFilter(filter, (long) value, expected);
        }
    }

    [Test]
    public void TestAndFilter()
    {
        var filter = Col.Named("x").IsEqualTo(3).And(Col.Named("y").IsEqualTo(4));

        foreach (var (xVal, yVal, expected) in new[]
                 {
                     (3, 4, true),
                     (4, 4, false),
                     (3, 3, false),
                 })
        {
            var partitionInfo = new PartitionInformation(
                new RecordBatch.Builder()
                    .Append("x", nullable: true, new Int64Array.Builder().Append(xVal))
                    .Append("y", nullable: true, new Int64Array.Builder().Append(yVal))
                    .Append("z", nullable: true, new Int64Array.Builder().Append(100))
                    .Build());

            Assert.That(filter.IncludePartition(partitionInfo), Is.EqualTo(expected));
        }
    }

    [Test]
    public void TestOrFilter()
    {
        var filter = Col.Named("x").IsEqualTo(3).Or(Col.Named("y").IsEqualTo(4));

        foreach (var (xVal, yVal, expected) in new[]
                 {
                     (3, 4, true),
                     (4, 4, true),
                     (3, 3, true),
                     (2, 2, false),
                 })
        {
            var partitionInfo = new PartitionInformation(
                new RecordBatch.Builder()
                    .Append("x", nullable: true, new Int64Array.Builder().Append(xVal))
                    .Append("y", nullable: true, new Int64Array.Builder().Append(yVal))
                    .Append("z", nullable: true, new Int64Array.Builder().Append(100))
                    .Build());

            Assert.That(filter.IncludePartition(partitionInfo), Is.EqualTo(expected));
        }
    }

    private static void TestIntColumnFilterWithAllTypes(IFilter filter, long? value, bool expected)
    {
            TestUInt8ColumnFilter(filter, (byte?) value, expected);
            TestUInt16ColumnFilter(filter, (ushort?) value, expected);
            TestUInt32ColumnFilter(filter, (uint?) value, expected);
            TestUInt64ColumnFilter(filter, (ulong?) value, expected);
            TestInt8ColumnFilter(filter, (sbyte?) value, expected);
            TestInt16ColumnFilter(filter, (short?) value, expected);
            TestInt32ColumnFilter(filter, (int?) value, expected);
            TestInt64ColumnFilter(filter, (long?) value, expected);
    }

    private static void TestUInt8ColumnFilter(IFilter filter, byte? value, bool expected) =>
        TestIntColumnFilter<byte, UInt8Array, UInt8Array.Builder>(filter, value, expected);

    private static void TestUInt16ColumnFilter(IFilter filter, ushort? value, bool expected) =>
        TestIntColumnFilter<ushort, UInt16Array, UInt16Array.Builder>(filter, value, expected);

    private static void TestUInt32ColumnFilter(IFilter filter, uint? value, bool expected) =>
        TestIntColumnFilter<uint, UInt32Array, UInt32Array.Builder>(filter, value, expected);

    private static void TestUInt64ColumnFilter(IFilter filter, ulong? value, bool expected) =>
        TestIntColumnFilter<ulong, UInt64Array, UInt64Array.Builder>(filter, value, expected);

    private static void TestInt8ColumnFilter(IFilter filter, sbyte? value, bool expected) =>
        TestIntColumnFilter<sbyte, Int8Array, Int8Array.Builder>(filter, value, expected);

    private static void TestInt16ColumnFilter(IFilter filter, short? value, bool expected) =>
        TestIntColumnFilter<short, Int16Array, Int16Array.Builder>(filter, value, expected);

    private static void TestInt32ColumnFilter(IFilter filter, int? value, bool expected) =>
        TestIntColumnFilter<int, Int32Array, Int32Array.Builder>(filter, value, expected);

    private static void TestInt64ColumnFilter(IFilter filter, long? value, bool expected) =>
        TestIntColumnFilter<long, Int64Array, Int64Array.Builder>(filter, value, expected);

    private static void TestIntColumnFilter<T, TArray, TBuilder>(IFilter filter, T? value, bool expected)
        where T : struct
        where TArray : IArrowArray
        where TBuilder : PrimitiveArrayBuilder<T, TArray, TBuilder>, new()
    {
        var builder = new TBuilder();
        if (value.HasValue)
        {
            builder.Append(value.Value);
        }
        else
        {
            builder.AppendNull();
        }
        var partitionInfo = new PartitionInformation(
            new RecordBatch.Builder()
                .Append("x", nullable: true, builder)
                .Build());

        Assert.That(filter.IncludePartition(partitionInfo), Is.EqualTo(expected));
    }
}
