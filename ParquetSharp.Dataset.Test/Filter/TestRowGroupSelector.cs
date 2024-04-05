using System;
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
    public void TestFilterPartitionColumn([Values] bool enableStatistics)
    {
        using var tmpDir = new DisposableDirectory();
        var filePath = tmpDir.AbsPath("test.parquet");

        var batch0 = GenerateBatch(0, 10);
        var batch1 = GenerateBatch(10, 20);
        WriteParquetFile(filePath, new[] { batch0, batch1 }, includeStats: enableStatistics);

        // Filter on an arbitrary field name that isn't found in the data file.
        // This will happen when filtering on a field from the partitioning schema.
        var filter = Col.Named("part").IsEqualTo(5);
        var rowGroupSelector = new RowGroupSelector(filter);

        using var reader = new FileReader(filePath);
        var rowGroups = rowGroupSelector.GetRequiredRowGroups(reader);
        // Null row groups indicate that all row groups should be read
        Assert.That(rowGroups, Is.Null);
    }

    [Test]
    public void TestFilterIntColumnValue([Values] bool enableStatistics)
    {
        using var tmpDir = new DisposableDirectory();
        var filePath = tmpDir.AbsPath("test.parquet");

        var batch0 = GenerateBatch(0, 10);
        var batch1 = GenerateBatch(10, 20);
        var batch2 = GenerateBatch(20, 30);
        WriteParquetFile(filePath, new[] { batch0, batch1, batch2 }, includeStats: enableStatistics);

        var filter = Col.Named("id").IsEqualTo(15);
        var rowGroupSelector = new RowGroupSelector(filter);

        using var reader = new FileReader(filePath);
        var rowGroups = rowGroupSelector.GetRequiredRowGroups(reader);
        var expectedRowGroups = enableStatistics ? new[] { 1 } : new[] { 0, 1, 2 };
        Assert.That(rowGroups, Is.EqualTo(expectedRowGroups));
    }

    [Test]
    public void TestFilterIntColumnRange([Values] bool enableStatistics)
    {
        using var tmpDir = new DisposableDirectory();
        var filePath = tmpDir.AbsPath("test.parquet");

        var batch0 = GenerateBatch(0, 10);
        var batch1 = GenerateBatch(10, 20);
        var batch2 = GenerateBatch(20, 30);
        WriteParquetFile(filePath, new[] { batch0, batch1, batch2 }, includeStats: enableStatistics);

        var filter = Col.Named("id").IsInRange(15, 25);
        var rowGroupSelector = new RowGroupSelector(filter);

        using var reader = new FileReader(filePath);
        var rowGroups = rowGroupSelector.GetRequiredRowGroups(reader);
        var expectedRowGroups = enableStatistics ? new[] { 1, 2 } : new[] { 0, 1, 2 };
        Assert.That(rowGroups, Is.EqualTo(expectedRowGroups));
    }

    [Test]
    public void TestFilterDateColumnRange([Values] bool enableStatistics)
    {
        using var tmpDir = new DisposableDirectory();
        var filePath = tmpDir.AbsPath("test.parquet");

        var batch0 = GenerateBatchWithDateColumn(new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 31));
        var batch1 = GenerateBatchWithDateColumn(new DateOnly(2024, 2, 1), new DateOnly(2024, 2, 29));
        var batch2 = GenerateBatchWithDateColumn(new DateOnly(2024, 3, 1), new DateOnly(2024, 3, 31));
        WriteParquetFile(filePath, new[] { batch0, batch1, batch2 }, includeStats: enableStatistics);

        var filter = Col.Named("date").IsInRange(new DateOnly(2024, 2, 5), new DateOnly(2024, 3, 5));
        var rowGroupSelector = new RowGroupSelector(filter);

        using var reader = new FileReader(filePath);
        var rowGroups = rowGroupSelector.GetRequiredRowGroups(reader);
        var expectedRowGroups = enableStatistics ? new[] { 1, 2 } : new[] { 0, 1, 2 };
        Assert.That(rowGroups, Is.EqualTo(expectedRowGroups));
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

    private static RecordBatch GenerateBatchWithDateColumn(DateOnly dateStart, DateOnly dateEnd)
    {
        const int rowsPerDate = 10;
        var builder = new RecordBatch.Builder();
        var dateValues = Enumerable.Range(0, dateEnd.DayNumber - dateStart.DayNumber)
            .SelectMany(days => Enumerable.Repeat(dateStart.AddDays(days), rowsPerDate))
            .ToArray();
        var xValues = Enumerable.Range(0, dateValues.Length).Select(i => i * 0.1f).ToArray();
        builder.Append("date", false, new Date32Array.Builder().Append(dateValues));
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
