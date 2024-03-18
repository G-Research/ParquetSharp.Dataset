using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;
using NUnit.Framework;
using ParquetSharp.Arrow;
using ParquetSharp.Dataset.Filter;

namespace ParquetSharp.Dataset.Test.Filter;

[TestFixture]
public class TestRowGroupSelector
{
    [Test]
    public void TestFilterPartitionColumn()
    {
        using var tmpDir = new DisposableDirectory();
        var filePath = tmpDir.AbsPath("test.parquet");

        var batch0 = GenerateBatch(0, 10);
        var batch1 = GenerateBatch(10, 20);
        WriteParquetFile(filePath, new[] { batch0, batch1 }, includeStats: true);

        var filter = Col.Named("part").IsEqualTo(5);
        var rowGroupSelector = new RowGroupSelector(filter);

        using var reader = new FileReader(filePath);
        var rowGroups = rowGroupSelector.GetRequiredRowGroups(reader);
        Assert.That(rowGroups, Is.Null);
    }

    [Test]
    public void TestNoStatistics()
    {
        using var tmpDir = new DisposableDirectory();
        var filePath = tmpDir.AbsPath("test.parquet");

        var batch0 = GenerateBatch(0, 10);
        var batch1 = GenerateBatch(10, 20);
        WriteParquetFile(filePath, new[] { batch0, batch1 }, includeStats: false);

        var filter = Col.Named("id").IsEqualTo(5);
        var rowGroupSelector = new RowGroupSelector(filter);

        using var reader = new FileReader(filePath);
        var rowGroups = rowGroupSelector.GetRequiredRowGroups(reader);
        Assert.That(rowGroups, Is.EqualTo(new[] { 0, 1 }));
    }

    [Test]
    public void TestFilterIntColumnValue()
    {
        using var tmpDir = new DisposableDirectory();
        var filePath = tmpDir.AbsPath("test.parquet");

        var batch0 = GenerateBatch(0, 10);
        var batch1 = GenerateBatch(10, 20);
        var batch2 = GenerateBatch(20, 30);
        WriteParquetFile(filePath, new[] { batch0, batch1, batch2 }, includeStats: true);

        var filter = Col.Named("id").IsEqualTo(15);
        var rowGroupSelector = new RowGroupSelector(filter);

        using var reader = new FileReader(filePath);
        var rowGroups = rowGroupSelector.GetRequiredRowGroups(reader);
        Assert.That(rowGroups, Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public void TestFilterIntColumnRange()
    {
        using var tmpDir = new DisposableDirectory();
        var filePath = tmpDir.AbsPath("test.parquet");

        var batch0 = GenerateBatch(0, 10);
        var batch1 = GenerateBatch(10, 20);
        var batch2 = GenerateBatch(20, 30);
        WriteParquetFile(filePath, new[] { batch0, batch1, batch2 }, includeStats: true);

        var filter = Col.Named("id").IsInRange(15, 25);
        var rowGroupSelector = new RowGroupSelector(filter);

        using var reader = new FileReader(filePath);
        var rowGroups = rowGroupSelector.GetRequiredRowGroups(reader);
        Assert.That(rowGroups, Is.EqualTo(new[] { 1, 2 }));
    }

    private static RecordBatch GenerateBatch(int idStart, int idEnd)
    {
        const int rowsPerId = 10;
        var builder = new RecordBatch.Builder();
        var idValues = Enumerable.Range(idStart, idEnd - idStart)
            .SelectMany(idVal => Enumerable.Repeat(idVal, rowsPerId))
            .ToArray();
        var xValues = Enumerable.Range(0, idValues.Length).Select(i => i * 0.1f).ToArray();
        builder.Append("id", false, new Int32Array.Builder().Append(idValues));
        builder.Append("x", false, new FloatArray.Builder().Append(xValues));
        return builder.Build();
    }

    private static void WriteParquetFile(string path, IReadOnlyList<RecordBatch> batches, bool includeStats)
    {
        using var writerPropertiesBuilder = new WriterPropertiesBuilder();
        if (includeStats)
        {
            writerPropertiesBuilder.EnableStatistics();
        }
        else
        {
            writerPropertiesBuilder.DisableStatistics();
        }

        using var writerProperties = writerPropertiesBuilder.Build();
        using var writer = new FileWriter(path, batches[0].Schema, writerProperties);
        foreach (var batch in batches)
        {
            writer.WriteRecordBatch(batch);
        }

        writer.Close();
    }
}
