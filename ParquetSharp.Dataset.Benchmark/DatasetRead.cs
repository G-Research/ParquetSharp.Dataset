using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using ParquetSharp.Arrow;
using ParquetSharp.Dataset.Partitioning;

namespace ParquetSharp.Dataset.Benchmark;

public class DatasetRead
{
    [GlobalSetup]
    public void Setup()
    {
        _datasetDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        _allFiles.Clear();

        var random = new Random(0);

        const int numGroups = 10;
        const int numDays = 10;
        const int rowsPerFile = 10_000;

        for (var groupNum = 0; groupNum < numGroups; ++groupNum)
        {
            var groupDirectory = Path.Join(_datasetDirectory, $"group=group-{groupNum}");
            Directory.CreateDirectory(groupDirectory);
            for (var dayNum = 0; dayNum < numDays; ++dayNum)
            {
                var dayDirectory = Path.Join(groupDirectory, $"day={dayNum}");
                Directory.CreateDirectory(dayDirectory);
                var filePath = Path.Join(dayDirectory, "data.parquet");
                WriteDataFile(filePath, rowsPerFile, random);
                _allFiles.Add(filePath);
            }
        }
    }

    private static void WriteDataFile(string filePath, int rowsPerFile, Random random)
    {
        var ids = new int[rowsPerFile];
        var ints = new long[rowsPerFile];
        var doubles = new double[rowsPerFile];

        for (var i = 0; i < rowsPerFile; ++i)
        {
            ids[i] = (int)random.NextInt64(0, 100);
            ints[i] = random.NextInt64(0, Int64.MaxValue);
            doubles[i] = random.NextDouble() * 100.0;
        }

        var batch = new RecordBatch.Builder()
            .Append("id", false, ab => ab.Int32(b => b.AppendRange(ids)))
            .Append("ints", true, ab => ab.Int64(b => b.AppendRange(ints)))
            .Append("doubles", true, ab => ab.Double(b => b.AppendRange(doubles)))
            .Build();

        using var writer = new FileWriter(filePath, batch.Schema);
        writer.WriteRecordBatch(batch);
        writer.Close();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        Directory.Delete(_datasetDirectory, recursive: true);
    }

    [Benchmark(Baseline = true)]
    public async Task<long> ReadAllFilesDirectly()
    {
        long rowsRead = 0;
        foreach (var filePath in _allFiles)
        {
            using var reader = new FileReader(filePath);
            using var batchReader = reader.GetRecordBatchReader();
            while (await batchReader.ReadNextRecordBatchAsync() is { } batch)
            {
                rowsRead += batch.Length;
            }
        }

        return rowsRead;
    }

    [Benchmark]
    public async Task<long> ReadAllData()
    {
        var dataset = new DatasetReader(_datasetDirectory, new HivePartitioning.Factory());
        using var reader = dataset.ToBatches();
        long rowsRead = 0;
        while (await reader.ReadNextRecordBatchAsync() is { } batch)
        {
            rowsRead += batch.Length;
        }

        return rowsRead;
    }

    [Benchmark]
    public async Task<long> FilterPartitions()
    {
        var dataset = new DatasetReader(_datasetDirectory, new HivePartitioning.Factory());
        var filter = Col.Named("group").IsEqualTo("group-2").And(Col.Named("day").IsEqualTo(2));
        using var reader = dataset.ToBatches(filter);
        long rowsRead = 0;
        while (await reader.ReadNextRecordBatchAsync() is { } batch)
        {
            rowsRead += batch.Length;
        }

        return rowsRead;
    }

    [Benchmark]
    public async Task<long> FilterFileData()
    {
        var dataset = new DatasetReader(_datasetDirectory, new HivePartitioning.Factory());
        var filter = Col.Named("id").IsEqualTo(5);
        using var reader = dataset.ToBatches(filter);
        long rowsRead = 0;
        while (await reader.ReadNextRecordBatchAsync() is { } batch)
        {
            rowsRead += batch.Length;
        }

        return rowsRead;
    }

    [Benchmark]
    public async Task<long> FilterToFileColumns()
    {
        var dataset = new DatasetReader(_datasetDirectory, new HivePartitioning.Factory());
        using var reader = dataset.ToBatches(columns: new[] { "id", "ints", "doubles" });
        long rowsRead = 0;
        while (await reader.ReadNextRecordBatchAsync() is { } batch)
        {
            rowsRead += batch.Length;
        }

        return rowsRead;
    }

    [Benchmark]
    public async Task<long> FilterToSingleColumn()
    {
        var dataset = new DatasetReader(_datasetDirectory, new HivePartitioning.Factory());
        using var reader = dataset.ToBatches(columns: new[] { "ints" });
        long rowsRead = 0;
        while (await reader.ReadNextRecordBatchAsync() is { } batch)
        {
            rowsRead += batch.Length;
        }

        return rowsRead;
    }

    private string _datasetDirectory = "";
    private readonly List<string> _allFiles = new();
}
