using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Types;
using NUnit.Framework;
using ParquetSharp.Arrow;

namespace ParquetSharp.Dataset.Test;

[TestFixture]
public static class TestFilterParquet
{
    [Test]
    public static async Task TestIntegerEqFilter()
    {
        var filter = Col.Named("x").IsEqualTo(10);
        await TestIntegerFilter(filter, x => x == 10);
    }

    [Test]
    public static async Task TestIntegerGtFilter()
    {
        var filter = Col.Named("x").IsGreaterThan(10);
        await TestIntegerFilter(filter, x => x > 10);
    }

    [Test]
    public static async Task TestIntegerGtEqFilter()
    {
        var filter = Col.Named("x").IsGreaterThanOrEqual(10);
        await TestIntegerFilter(filter, x => x >= 10);
    }

    [Test]
    public static async Task TestIntegerLtFilter()
    {
        var filter = Col.Named("x").IsLessThan(-10);
        await TestIntegerFilter(filter, x => x < -10);
    }

    [Test]
    public static async Task TestIntegerLtEqFilter()
    {
        var filter = Col.Named("x").IsLessThanOrEqual(-10);
        await TestIntegerFilter(filter, x => x <= -10);
    }

    private static async Task TestIntegerFilter(IFilter filter, Func<int, bool> includeValue)
    {
        using var datasetDir = new DisposableDirectory();
        var filePath = datasetDir.AbsPath("test_data.parquet");

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", new Int32Type(), true))
            .Build();
        using var writer = new FileWriter(filePath, schema);

        const int numBatches = 100;
        const int batchSize = 10;
        var expectedValues = new List<int?>();

        var random = new Random(0);
        var batchValues = new int?[batchSize];
        for (var batchIdx = 0; batchIdx < numBatches; ++batchIdx)
        {
            for (var rowIdx = 0; rowIdx < batchSize; ++rowIdx)
            {
                if (random.NextSingle() < 0.01)
                {
                    batchValues[rowIdx] = null;
                }
                else
                {
                    var value = random.Next(-20, 20);
                    batchValues[rowIdx] = value;
                    if (includeValue(value))
                    {
                        expectedValues.Add(value);
                    }
                }
            }

            var builder = new RecordBatch.Builder();
            var arrayBuilder = new Int32Array.Builder();
            foreach (var x in batchValues)
            {
                arrayBuilder.Append(x);
            }

            builder.Append("x", true, arrayBuilder);
            var batch = builder.Build();
            writer.WriteRecordBatch(batch);
        }

        writer.Close();

        var dataset = new DatasetReader(datasetDir.DirectoryPath);
        using var reader = dataset.ToBatches(filter);
        var valuesRead = new List<int?>();
        while (await reader.ReadNextRecordBatchAsync() is { } batch)
        {
            var filteredBatchValues = batch.Column(0) as Int32Array;
            foreach (var value in filteredBatchValues!)
            {
                valuesRead.Add(value);
            }
        }

        Assert.That(valuesRead, Is.EqualTo(expectedValues));
    }
}
