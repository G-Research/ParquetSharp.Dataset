using System.Linq;
using Apache.Arrow;
using NUnit.Framework;

namespace ParquetSharp.Dataset.Test.Filter;

[TestFixture]
public class TestFilterCombination
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
    public void TestAndFilterIncludePartition()
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
    public void TestOrFilterIncludePartition()
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

    [Test]
    public void TestAndFilterMask()
    {
        var filter = Col.Named("x").IsEqualTo(3).And(Col.Named("y").IsEqualTo(4));
        const int numRows = 128;

        var xValues = Enumerable.Range(0, numRows).Select(i => i % 2 == 0 ? 3 : 1).ToArray();
        var yValues = Enumerable.Range(0, numRows).Select(i => i % 4 < 2 ? (long)4 : 2).ToArray();
        var batch = new RecordBatch.Builder()
            .Append("x", false, new Int32Array.Builder().Append(xValues))
            .Append("y", false, new Int64Array.Builder().Append(yValues))
            .Build();

        var mask = filter.ComputeMask(batch);

        Assert.That(mask, Is.Not.Null);
        Assert.That(mask!.IncludedCount, Is.EqualTo(numRows / 4));
        for (var i = 0; i < numRows; ++i)
        {
            Assert.That(BitUtility.GetBit(mask.Mask.Span, i), Is.EqualTo(i % 4 == 0));
        }
    }

    [Test]
    public void TestAndFilterMaskWithNoMatchingColumns()
    {
        var filter = Col.Named("x").IsEqualTo(3).And(Col.Named("y").IsEqualTo(4));
        const int numRows = 128;

        var zValues = Enumerable.Range(0, numRows).ToArray();
        var batch = new RecordBatch.Builder()
            .Append("z", false, new Int32Array.Builder().Append(zValues))
            .Build();

        var mask = filter.ComputeMask(batch);

        Assert.That(mask, Is.Null);
    }

    [Test]
    public void TestAndFilterMaskWithLhsOnly()
    {
        var filter = Col.Named("x").IsEqualTo(3).And(Col.Named("y").IsEqualTo(4));
        const int numRows = 128;

        var xValues = Enumerable.Range(0, numRows).Select(i => i % 2 == 0 ? 3 : 1).ToArray();
        var batch = new RecordBatch.Builder()
            .Append("x", false, new Int32Array.Builder().Append(xValues))
            .Build();

        var mask = filter.ComputeMask(batch);

        Assert.That(mask, Is.Not.Null);
        Assert.That(mask!.IncludedCount, Is.EqualTo(numRows / 2));
        for (var i = 0; i < numRows; ++i)
        {
            Assert.That(BitUtility.GetBit(mask.Mask.Span, i), Is.EqualTo(i % 2 == 0));
        }
    }

    [Test]
    public void TestAndFilterMaskWithRhsOnly()
    {
        var filter = Col.Named("x").IsEqualTo(3).And(Col.Named("y").IsEqualTo(4));
        const int numRows = 128;

        var yValues = Enumerable.Range(0, numRows).Select(i => i % 4 < 2 ? (long)4 : 2).ToArray();
        var batch = new RecordBatch.Builder()
            .Append("y", false, new Int64Array.Builder().Append(yValues))
            .Build();

        var mask = filter.ComputeMask(batch);

        Assert.That(mask, Is.Not.Null);
        Assert.That(mask!.IncludedCount, Is.EqualTo(numRows / 2));
        for (var i = 0; i < numRows; ++i)
        {
            Assert.That(BitUtility.GetBit(mask.Mask.Span, i), Is.EqualTo(i % 4 < 2));
        }
    }

    [Test]
    public void TestOrFilterMask()
    {
        var filter = Col.Named("x").IsEqualTo(3).Or(Col.Named("y").IsEqualTo(4));
        const int numRows = 128;

        var xValues = Enumerable.Range(0, numRows).Select(i => i % 2 == 0 ? 3 : 1).ToArray();
        var yValues = Enumerable.Range(0, numRows).Select(i => i % 4 < 2 ? (long)4 : 2).ToArray();
        var batch = new RecordBatch.Builder()
            .Append("x", false, new Int32Array.Builder().Append(xValues))
            .Append("y", false, new Int64Array.Builder().Append(yValues))
            .Build();

        var mask = filter.ComputeMask(batch);

        Assert.That(mask, Is.Not.Null);
        Assert.That(mask!.IncludedCount, Is.EqualTo(3 * numRows / 4));
        for (var i = 0; i < numRows; ++i)
        {
            Assert.That(BitUtility.GetBit(mask.Mask.Span, i), Is.EqualTo(i % 4 < 3));
        }
    }

    [Test]
    public void TestOrFilterMaskWithNoMatchingColumns()
    {
        var filter = Col.Named("x").IsEqualTo(3).Or(Col.Named("y").IsEqualTo(4));
        const int numRows = 128;

        var zValues = Enumerable.Range(0, numRows).ToArray();
        var batch = new RecordBatch.Builder()
            .Append("z", false, new Int32Array.Builder().Append(zValues))
            .Build();

        var mask = filter.ComputeMask(batch);

        Assert.That(mask, Is.Null);
    }

    [Test]
    public void TestOrFilterMaskWithLhsOnly()
    {
        var filter = Col.Named("x").IsEqualTo(3).Or(Col.Named("y").IsEqualTo(4));
        const int numRows = 128;

        var xValues = Enumerable.Range(0, numRows).Select(i => i % 2 == 0 ? 3 : 1).ToArray();
        var batch = new RecordBatch.Builder()
            .Append("x", false, new Int32Array.Builder().Append(xValues))
            .Build();

        var mask = filter.ComputeMask(batch);

        Assert.That(mask, Is.Null);
    }

    [Test]
    public void TestOrFilterMaskWithRhsOnly()
    {
        var filter = Col.Named("x").IsEqualTo(3).Or(Col.Named("y").IsEqualTo(4));
        const int numRows = 128;

        var yValues = Enumerable.Range(0, numRows).Select(i => i % 4 < 2 ? (long)4 : 2).ToArray();
        var batch = new RecordBatch.Builder()
            .Append("y", false, new Int64Array.Builder().Append(yValues))
            .Build();

        var mask = filter.ComputeMask(batch);

        Assert.That(mask, Is.Null);
    }
}
