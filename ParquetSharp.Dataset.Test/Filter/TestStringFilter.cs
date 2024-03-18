using Apache.Arrow;
using NUnit.Framework;

namespace ParquetSharp.Dataset.Test.Filter;

[TestFixture]
public class TestStringFilter
{
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
        var filter = Col.Named("x").IsIn(new[] { "abc", "def" });

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
}
