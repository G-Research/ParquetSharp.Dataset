using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using NUnit.Framework;
using ParquetSharp.Arrow;
using ParquetSharp.Dataset.Partitioning;

namespace ParquetSharp.Dataset.Test;

[TestFixture]
public class TestDatasetReader
{
    [Test]
    public async Task TestReadMultipleFilesWithExplicitSchema()
    {
        using var tmpDir = new DisposableDirectory();
        using var batch0 = GenerateBatch(0);
        using var batch1 = GenerateBatch(1);
        WriteParquetFile(tmpDir.AbsPath("data0.parquet"), batch0);
        WriteParquetFile(tmpDir.AbsPath("data1.parquet"), batch1);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", new Int32Type(), false))
            .Field(new Field("x", new FloatType(), false))
            .Build();
        var dataset = new DatasetReader(
            tmpDir.DirectoryPath,
            new NoPartitioning(),
            schema: schema);
        using var reader = dataset.ToBatches();
        await VerifyData(reader, new Dictionary<int, int> {{0, 10}, {1, 10}});
    }

    [Test]
    public async Task TestReadMultipleFilesWithHivePartitioning()
    {
        using var tmpDir = new DisposableDirectory();
        using var batch0 = GenerateBatch(0);
        using var batch1 = GenerateBatch(1);
        using var batch2 = GenerateBatch(2);
        using var batch3 = GenerateBatch(3);
        WriteParquetFile(tmpDir.AbsPath("part=a/data0.parquet"), batch0);
        WriteParquetFile(tmpDir.AbsPath("part=a/data1.parquet"), batch1);
        WriteParquetFile(tmpDir.AbsPath("part=b/data0.parquet"), batch2);
        WriteParquetFile(tmpDir.AbsPath("part=b/data1.parquet"), batch3);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("part", new StringType(), false))
            .Field(new Field("id", new Int32Type(), false))
            .Field(new Field("x", new FloatType(), false))
            .Build();
        var partitioning = new HivePartitioning(
                new Apache.Arrow.Schema.Builder()
                    .Field(new Field("part", new StringType(), false))
                    .Build());
        var dataset = new DatasetReader(
            tmpDir.DirectoryPath,
            partitioning,
            schema: schema);

        // Read all data
        using var reader = dataset.ToBatches();
        await VerifyData(
            reader,
            new Dictionary<int, int> {{0, 10}, {1, 10}, {2, 10}, {3, 10}},
            new Dictionary<string, int> {{"a", 20}, {"b", 20}});

        // Read filtered on partition
        var filter = Col.Named("part").IsEqualTo("b");
        using var filteredReader = dataset.ToBatches(filter);
        await VerifyData(
            filteredReader,
            new Dictionary<int, int> {{2, 10}, {3, 10}},
            new Dictionary<string, int> {{"b", 20}});
    }

    [Test]
    public async Task TestReadColumnSubset()
    {
        using var tmpDir = new DisposableDirectory();
        using var batch0 = GenerateBatch(0);
        using var batch1 = GenerateBatch(1);
        WriteParquetFile(tmpDir.AbsPath("part=a/data0.parquet"), batch0);
        WriteParquetFile(tmpDir.AbsPath("part=b/data0.parquet"), batch1);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("part", new StringType(), false))
            .Field(new Field("id", new Int32Type(), false))
            .Field(new Field("x", new FloatType(), false))
            .Build();
        var partitioning = new HivePartitioning(
                new Apache.Arrow.Schema.Builder()
                    .Field(new Field("part", new StringType(), false))
                    .Build());
        var dataset = new DatasetReader(
            tmpDir.DirectoryPath,
            partitioning,
            schema: schema);

        // Read all files, but a subset of columns
        using var reader = dataset.ToBatches(columns: new [] {"id", "part"});
        Assert.That(reader.Schema.FieldsList.Select(f => f.Name), Is.EqualTo(new [] {"id", "part"}));

        while (await reader.ReadNextRecordBatchAsync() is { } batch)
        {
            using (batch)
            {
                Assert.That(batch.Schema.FieldsList.Select(f => f.Name), Is.EqualTo(new [] {"id", "part"}));
                Assert.That(batch.ColumnCount, Is.EqualTo(2));
                Assert.That(batch.Column(0), Is.InstanceOf<Int32Array>());
                Assert.That(batch.Column(1), Is.InstanceOf<StringArray>());
            }
        }
    }

    [Test]
    public async Task TestReadExcludingPartitionColumn()
    {
        using var tmpDir = new DisposableDirectory();
        using var batch0 = GenerateBatch(0);
        using var batch1 = GenerateBatch(1);
        WriteParquetFile(tmpDir.AbsPath("part=a/data0.parquet"), batch0);
        WriteParquetFile(tmpDir.AbsPath("part=b/data0.parquet"), batch1);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("part", new StringType(), false))
            .Field(new Field("id", new Int32Type(), false))
            .Field(new Field("x", new FloatType(), false))
            .Build();
        var partitioning = new HivePartitioning(
                new Apache.Arrow.Schema.Builder()
                    .Field(new Field("part", new StringType(), false))
                    .Build());
        var dataset = new DatasetReader(
            tmpDir.DirectoryPath,
            partitioning,
            schema: schema);

        // Read all files, but don't create a column for the partitioning field
        using var reader = dataset.ToBatches(columns: new [] {"id", "x"});
        Assert.That(reader.Schema.FieldsList.Select(f => f.Name), Is.EqualTo(new [] {"id", "x"}));

        while (await reader.ReadNextRecordBatchAsync() is { } batch)
        {
            using (batch)
            {
                Assert.That(batch.Schema.FieldsList.Select(f => f.Name), Is.EqualTo(new [] {"id", "x"}));
                Assert.That(batch.ColumnCount, Is.EqualTo(2));
                Assert.That(batch.Column(0), Is.InstanceOf<Int32Array>());
                Assert.That(batch.Column(1), Is.InstanceOf<FloatArray>());
            }
        }
    }

    [Test]
    public void TestInvalidColumnName()
    {
        using var tmpDir = new DisposableDirectory();
        using var batch0 = GenerateBatch(0);
        using var batch1 = GenerateBatch(1);
        WriteParquetFile(tmpDir.AbsPath("part=a/data0.parquet"), batch0);
        WriteParquetFile(tmpDir.AbsPath("part=b/data0.parquet"), batch1);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("part", new StringType(), false))
            .Field(new Field("id", new Int32Type(), false))
            .Field(new Field("x", new FloatType(), false))
            .Build();
        var partitioning = new HivePartitioning(
                new Apache.Arrow.Schema.Builder()
                    .Field(new Field("part", new StringType(), false))
                    .Build());
        var dataset = new DatasetReader(
            tmpDir.DirectoryPath,
            partitioning,
            schema: schema);

        var exception = Assert.Throws<ArgumentException>(
            () => dataset.ToBatches(columns: new[] {"part", "id", "nonexistent"}));
        Assert.That(exception!.Message, Does.Contain("'nonexistent'"));
    }

    [Test]
    public void TestGetSchemaWithNoDataFiles()
    {
        using var tmpDir = new DisposableDirectory();

        var partitioning = new HivePartitioning(
                new Apache.Arrow.Schema.Builder()
                    .Field(new Field("part", new StringType(), false))
                    .Build());
        var dataset = new DatasetReader(
            tmpDir.DirectoryPath,
            partitioning);

        var schema = dataset.Schema;

        Assert.That(schema.FieldsList.Count, Is.EqualTo(1));

        var partField = schema.GetFieldByName("part");
        Assert.That(partField.DataType.TypeId, Is.EqualTo(ArrowTypeId.String));
        Assert.That(partField.IsNullable, Is.False);
    }

    [Test]
    public void TestGetSchemaFromDataFileAndPartitioning()
    {
        using var tmpDir = new DisposableDirectory();
        using var batch0 = GenerateBatch(0);
        WriteParquetFile(tmpDir.AbsPath("part=a/data0.parquet"), batch0);

        var partitioning = new HivePartitioning(
                new Apache.Arrow.Schema.Builder()
                    .Field(new Field("part", new StringType(), false))
                    .Build());
        var dataset = new DatasetReader(
            tmpDir.DirectoryPath,
            partitioning);

        var schema = dataset.Schema;

        Assert.That(schema.FieldsList.Count, Is.EqualTo(3));

        var partField = schema.GetFieldByName("part");
        Assert.That(partField.DataType.TypeId, Is.EqualTo(ArrowTypeId.String));
        Assert.That(partField.IsNullable, Is.False);

        var idField = schema.GetFieldByName("id");
        Assert.That(idField.DataType.TypeId, Is.EqualTo(ArrowTypeId.Int32));
        Assert.That(idField.IsNullable, Is.False);

        var xField = schema.GetFieldByName("x");
        Assert.That(xField.DataType.TypeId, Is.EqualTo(ArrowTypeId.Float));
        Assert.That(xField.IsNullable, Is.False);
    }

    [Test]
    public void TestGetSchemaFromDataFileAndPartitioningFactory()
    {
        using var tmpDir = new DisposableDirectory();
        using var batch0 = GenerateBatch(0);
        WriteParquetFile(tmpDir.AbsPath("part=a/data0.parquet"), batch0);

        var partitioningFactory = new HivePartitioning.Factory();
        var dataset = new DatasetReader(
            tmpDir.DirectoryPath,
            partitioningFactory);

        var schema = dataset.Schema;

        Assert.That(schema.FieldsList.Count, Is.EqualTo(3));

        var partField = schema.GetFieldByName("part");
        Assert.That(partField.DataType.TypeId, Is.EqualTo(ArrowTypeId.String));
        Assert.That(partField.IsNullable, Is.True);

        var idField = schema.GetFieldByName("id");
        Assert.That(idField.DataType.TypeId, Is.EqualTo(ArrowTypeId.Int32));
        Assert.That(idField.IsNullable, Is.False);

        var xField = schema.GetFieldByName("x");
        Assert.That(xField.DataType.TypeId, Is.EqualTo(ArrowTypeId.Float));
        Assert.That(xField.IsNullable, Is.False);
    }

    [Test]
    public void TestInferPartitioningSchemaFromExplicitSchema()
    {
        using var tmpDir = new DisposableDirectory();
        using var batch0 = GenerateBatch(0);
        WriteParquetFile(tmpDir.AbsPath("part=a/data0.parquet"), batch0);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("part", new StringType(), false))
            .Field(new Field("id", new Int32Type(), false))
            .Field(new Field("x", new FloatType(), false))
            .Build();

        var partitioningFactory = new HivePartitioning.Factory();
        var dataset = new DatasetReader(
            tmpDir.DirectoryPath,
            partitioningFactory,
            schema: schema);

        var partitionSchema = dataset.Partitioning.Schema;

        Assert.That(partitionSchema.FieldsList.Count, Is.EqualTo(1));

        var partField = partitionSchema.GetFieldByName("part");
        Assert.That(partField.DataType.TypeId, Is.EqualTo(ArrowTypeId.String));
        Assert.That(partField.IsNullable, Is.False);
    }

    private static async Task VerifyData(
        IArrowArrayStream arrayStream,
        Dictionary<int, int> expectedRowCountsById,
        Dictionary<string, int>? expectedRowCountsByPart = null)
    {
        var rowCountsById = new Dictionary<int, int>();
        var rowCountsByPart = new Dictionary<string, int>();

        while (await arrayStream.ReadNextRecordBatchAsync() is { } batch)
        {
            using (batch)
            {
                var idValues = batch.Column("id") as Int32Array;
                var xValues = batch.Column("x") as FloatArray;
                Assert.That(idValues, Is.Not.Null);
                Assert.That(xValues, Is.Not.Null);
                StringArray? partValues = null;
                if (expectedRowCountsByPart != null)
                {
                    partValues = batch.Column("part") as StringArray;
                    Assert.That(partValues, Is.Not.Null);
                }
                for (var i = 0; i < batch.Length; ++i)
                {
                    var id = idValues!.GetValue(i)!.Value;
                    var x = xValues!.GetValue(i)!.Value;
                    if (!rowCountsById.TryAdd(id, 1))
                    {
                        rowCountsById[id] += 1;
                    }
                    Assert.That(x, Is.GreaterThanOrEqualTo((float) id));
                    Assert.That(x, Is.LessThan((float) id + 1));

                    if (partValues != null)
                    {
                        var part = partValues.GetString(i);
                        if (!rowCountsByPart.TryAdd(part, 1))
                        {
                            rowCountsByPart[part] += 1;
                        }
                    }
                }
            }
        }

        Assert.That(rowCountsById.Count, Is.EqualTo(expectedRowCountsById.Count));
        foreach (var kvp in expectedRowCountsById)
        {
            Assert.That(rowCountsById[kvp.Key], Is.EqualTo(kvp.Value));
        }

        if (expectedRowCountsByPart != null)
        {
            Assert.That(rowCountsByPart.Count, Is.EqualTo(expectedRowCountsByPart.Count));
            foreach (var kvp in expectedRowCountsByPart)
            {
                Assert.That(rowCountsByPart[kvp.Key], Is.EqualTo(kvp.Value));
            }
        }
    }

    private static RecordBatch GenerateBatch(int id, int numRows=10)
    {
        var builder = new RecordBatch.Builder();
        var idValues = Enumerable.Repeat(id, numRows).ToArray();
        builder.Append("id", false, new Int32Array.Builder().Append(idValues));
        var xValues = Enumerable.Range(0, numRows).Select(x => id + x / (float) numRows).ToArray();
        builder.Append("x", false, new FloatArray.Builder().Append(xValues));
        return builder.Build();
    }

    private static void WriteParquetFile(string path, RecordBatch batch)
    {
        using var writer = new FileWriter(path, batch.Schema);
        writer.WriteRecordBatch(batch);
        writer.Close();
    }
}
