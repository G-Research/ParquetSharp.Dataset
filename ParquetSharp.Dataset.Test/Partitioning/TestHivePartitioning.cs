using Apache.Arrow;
using Apache.Arrow.Types;
using NUnit.Framework;
using ParquetSharp.Dataset.Partitioning;

namespace ParquetSharp.Dataset.Test.Partitioning;

public class TestHivePartitioning
{
    [Test]
    public void TestEmptySchema()
    {
        var partitioningSchema = new Apache.Arrow.Schema.Builder().Build();
        var partitioning = new HivePartitioning(partitioningSchema);

        var subsetData = partitioning.Parse(System.Array.Empty<string>()).Batch;

        Assert.That(subsetData.Length, Is.EqualTo(1));
        Assert.That(subsetData.Schema.FieldsList.Count, Is.EqualTo(0));
    }

    [Test]
    public void TestSingleIntPartitionField()
    {
        var partitioningSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", new Int64Type(), nullable: true))
            .Build();
        var partitioning = new HivePartitioning(partitioningSchema);

        var subsetData = partitioning.Parse(new [] {"x=3"}).Batch;

        Assert.That(subsetData.Length, Is.EqualTo(1));
        Assert.That(subsetData.Schema.FieldsList.Count, Is.EqualTo(1));
        var column = subsetData.Column("x") as Int64Array;
        Assert.That(column, Is.Not.Null);
        Assert.That(column!.GetValue(0), Is.EqualTo(3));
    }

    [Test]
    public void TestSingleStringPartitionField()
    {
        var partitioningSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", new StringType(), nullable: true))
            .Build();
        var partitioning = new HivePartitioning(partitioningSchema);

        // Should respect type even if this value can be interpreted as int
        var subsetData = partitioning.Parse(new [] {"x=3"}).Batch;

        Assert.That(subsetData.Length, Is.EqualTo(1));
        Assert.That(subsetData.Schema.FieldsList.Count, Is.EqualTo(1));
        var column = subsetData.Column("x") as StringArray;
        Assert.That(column, Is.Not.Null);
        Assert.That(column!.GetString(0), Is.EqualTo("3"));
    }

    [Test]
    public void TestMultiplePartitionField()
    {
        var partitioningSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", new Int64Type(), nullable: true))
            .Field(new Field("y", new StringType(), nullable: true))
            .Build();
        var partitioning = new HivePartitioning(partitioningSchema);

        // Should respect type even if this value can be interpreted as int
        var subsetData = partitioning.Parse(new [] {"y=hello", "x=4"}).Batch;

        Assert.That(subsetData.Length, Is.EqualTo(1));
        Assert.That(subsetData.Schema.FieldsList.Count, Is.EqualTo(2));

        var xColumn = subsetData.Column("x") as Int64Array;
        Assert.That(xColumn, Is.Not.Null);
        Assert.That(xColumn!.GetValue(0), Is.EqualTo(4));

        var yColumn = subsetData.Column("y") as StringArray;
        Assert.That(yColumn, Is.Not.Null);
        Assert.That(yColumn!.GetString(0), Is.EqualTo("hello"));
    }

    [Test]
    public void TestNullValue()
    {
        var partitioningSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", new Int64Type(), nullable: true))
            .Build();
        var partitioning = new HivePartitioning(partitioningSchema);

        var subsetData = partitioning.Parse(new [] {"x=__HIVE_DEFAULT_PARTITION__"}).Batch;

        Assert.That(subsetData.Length, Is.EqualTo(1));
        Assert.That(subsetData.Schema.FieldsList.Count, Is.EqualTo(1));
        var column = subsetData.Column("x") as Int64Array;
        Assert.That(column, Is.Not.Null);
        Assert.That(column!.GetValue(0), Is.Null);
    }

    [Test]
    public void TestNullValueForNonNullableField()
    {
        var partitioningSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", new Int64Type(), nullable: false))
            .Build();
        var partitioning = new HivePartitioning(partitioningSchema);

        var exception = Assert.Throws<ArgumentException>(
            () => partitioning.Parse(new[] {"x=__HIVE_DEFAULT_PARTITION__"}));
        Assert.That(exception!.ParamName, Is.EqualTo("pathComponents"));
    }

    [Test]
    public void TestUnexpectedFieldWithEmptySchema()
    {
        var partitioningSchema = new Apache.Arrow.Schema.Builder().Build();
        var partitioning = new HivePartitioning(partitioningSchema);

        var exception = Assert.Throws<ArgumentException>(
            () => partitioning.Parse(new[] {"column=value"}));
        Assert.That(exception!.ParamName, Is.EqualTo("pathComponents"));
    }

    [Test]
    public void TestSplitOnInvalidFieldName()
    {
        var partitioningSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", new Int64Type(), nullable: true))
            .Build();
        var partitioning = new HivePartitioning(partitioningSchema);

        var exception = Assert.Throws<ArgumentException>(
            () => partitioning.Parse(new[] {"y=3"}));
        Assert.That(exception!.ParamName, Is.EqualTo("pathComponents"));
        Assert.That(exception.Message, Does.Contain("'y'"));
    }

    [Test]
    public void TestInvalidSyntax()
    {
        var partitioningSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", new Int64Type(), nullable: true))
            .Build();
        var partitioning = new HivePartitioning(partitioningSchema);

        var exception = Assert.Throws<ArgumentException>(
            () => partitioning.Parse(new[] {"x3"}));
        Assert.That(exception!.ParamName, Is.EqualTo("pathComponents"));
        Assert.That(exception.Message, Does.Contain("'x3'"));
    }

    [Test]
    public void TestUrlEncodedValues()
    {
        var partitioningSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x =#!/", new StringType(), nullable: true))
            .Build();
        var partitioning = new HivePartitioning(partitioningSchema);

        var subsetData = partitioning.Parse(new [] {"x%20%3d%23%21%2f=%3a%3b%2b%27%22"}).Batch;

        Assert.That(subsetData.Length, Is.EqualTo(1));
        Assert.That(subsetData.Schema.FieldsList.Count, Is.EqualTo(1));
        var column = subsetData.Column("x =#!/") as StringArray;
        Assert.That(column, Is.Not.Null);
        Assert.That(column!.GetString(0), Is.EqualTo(":;+'\""));
    }
}
