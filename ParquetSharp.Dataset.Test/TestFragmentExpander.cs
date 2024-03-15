using Apache.Arrow;
using Apache.Arrow.Types;
using NUnit.Framework;

namespace ParquetSharp.Dataset.Test;

[TestFixture]
public class TestFragmentExpander
{
    [Test]
    public void TestNoOpExpand()
    {
        var datasetSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", new Int32Type(), false))
            .Field(new Field("y", new Int32Type(), false))
            .Build();

        var fragmentData = new RecordBatch.Builder()
            .Append("x", false, new Int32Array.Builder().Append(new[] {0, 1, 2, 3, 4}))
            .Append("y", false, new Int32Array.Builder().Append(new[] {5, 6, 7, 8, 9}))
            .Build();

        var expander = new FragmentExpander(datasetSchema);

        var expanded = expander.ExpandBatch(fragmentData, PartitionInformation.Empty);

        for (var colIdx = 0; colIdx < fragmentData.ColumnCount; ++colIdx)
        {
            Assert.That(expanded.Column(colIdx), Is.SameAs(fragmentData.Column(colIdx)));
        }
    }

    [Test]
    public void TestAddDataFromPartition()
    {
        var datasetSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", new Int32Type(), false))
            .Field(new Field("y", new Int32Type(), false))
            .Field(new Field("z", new StringType(), false))
            .Build();

        var batchLength = 5;
        var fragmentData = new RecordBatch.Builder()
            .Append("x", false, new Int32Array.Builder().Append(Enumerable.Range(0, batchLength).ToArray()))
            .Build();

        var partitionData = new RecordBatch.Builder()
            .Append("y", false, new Int32Array.Builder().Append(5))
            .Append("z", false, new StringArray.Builder().Append("abc"))
            .Build();

        var expander = new FragmentExpander(datasetSchema);

        var expanded = expander.ExpandBatch(fragmentData, new PartitionInformation(partitionData));

        Assert.That(expanded.Schema.FieldsList.Count, Is.EqualTo(3));

        var xArray = expanded.Column("x") as Int32Array;
        Assert.That(xArray, Is.Not.Null);

        var yArray = expanded.Column("y") as Int32Array;
        Assert.That(yArray, Is.Not.Null);
        Assert.That(yArray!.Length, Is.EqualTo(batchLength));

        var zArray = expanded.Column("z") as StringArray;
        Assert.That(zArray, Is.Not.Null);
        Assert.That(zArray!.Length, Is.EqualTo(batchLength));

        for (var i = 0; i < batchLength; ++i)
        {
            Assert.That(yArray.GetValue(i), Is.EqualTo(5));
            Assert.That(zArray.GetString(i), Is.EqualTo("abc"));
        }
    }

    [Test]
    public void TestMissingField()
    {
        var datasetSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", new Int32Type(), false))
            .Field(new Field("y", new Int32Type(), false))
            .Field(new Field("z", new StringType(), false))
            .Build();

        var batchLength = 5;
        var fragmentData = new RecordBatch.Builder()
            .Append("x", false, new Int32Array.Builder().Append(Enumerable.Range(0, batchLength).ToArray()))
            .Build();

        var partitionData = new RecordBatch.Builder()
            .Append("y", false, new Int32Array.Builder().Append(5))
            .Build();

        var expander = new FragmentExpander(datasetSchema);

        var exception = Assert.Throws<Exception>(
            () => expander.ExpandBatch(fragmentData, new PartitionInformation(partitionData)));
        Assert.That(exception!.Message, Does.Contain("'z'"));
    }
}
