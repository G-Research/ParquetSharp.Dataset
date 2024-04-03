using Apache.Arrow;
using NUnit.Framework;

namespace ParquetSharp.Dataset.Test.Filter;

[TestFixture]
public class TestStringFilter
{
    [Test]
    public void TestStringEqualityFilterIncludePartition()
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
    public void TestNullStringEqualityFilterIncludePartition()
    {
        var filter = Col.Named("x").IsEqualTo(null);

        var partitionInfo = new PartitionInformation(
            new RecordBatch.Builder()
                .Append("x", nullable: true, new StringArray.Builder().AppendNull())
                .Build());

        Assert.That(filter.IncludePartition(partitionInfo));

        partitionInfo = new PartitionInformation(
            new RecordBatch.Builder()
                .Append("x", nullable: true, new StringArray.Builder().Append("abc"))
                .Build());

        Assert.That(filter.IncludePartition(partitionInfo), Is.False);
    }

    [TestCase("def", true)]
    [TestCase("ghi", false)]
    [TestCase(null, true)]
    public void TestStringSetFilterIncludePartition(string? testValue, bool shouldBeIncluded)
    {
        var filter = Col.Named("x").IsIn(new[] { "abc", "def", null });

        var partitionInfo = new PartitionInformation(
            new RecordBatch.Builder()
                .Append("x", nullable: true, new StringArray.Builder().Append(testValue))
                .Build());

        Assert.That(filter.IncludePartition(partitionInfo), Is.EqualTo(shouldBeIncluded));
    }

    [Test]
    public void TestStringSetFilterComputeMask()
    {
        var filter = Col.Named("x").IsIn(new[] { "a", "c" });

        var batch = new RecordBatch.Builder()
            .Append("x", nullable: true, new StringArray.Builder().AppendRange(new[]
            {
                "a",
                "b",
                "c",
                null,
                "d",
                "e",
                "f",
                "c",
                "a",
                "g",
            }))
            .Build();

        var mask = filter.ComputeMask(batch);

        Assert.That(mask, Is.Not.Null);
        Assert.That(mask!.IncludedCount, Is.EqualTo(4));
        var expected = new bool[] { true, false, true, false, false, false, false, true, true, false };
        for (var i = 0; i < expected.Length; ++i)
        {
            Assert.That(BitUtility.GetBit(mask.Mask.Span, i), Is.EqualTo(expected[i]));
        }
    }
}
