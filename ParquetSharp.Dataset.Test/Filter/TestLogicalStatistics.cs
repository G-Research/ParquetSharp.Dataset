using System;
using System.Linq;
using NUnit.Framework;
using ParquetSharp.Dataset.Filter;

namespace ParquetSharp.Dataset.Test.Filter;

[TestFixture]
public class TestLogicalStatistics
{
    [Test]
    public void TestCreateSByteStatistics()
    {
        var rowGroupValues = new[]
        {
            new sbyte?[] { -2, -4, 5, null, 1, 0 },
            new sbyte?[] { -6, 7, 4 },
            new sbyte?[] { 2, null, 3, 2 },
            new sbyte?[] { sbyte.MaxValue, sbyte.MinValue },
        };
        var expectedMin = new sbyte[] { -4, -6, 2, sbyte.MinValue };
        var expectedMax = new sbyte[] { 5, 7, 3, sbyte.MaxValue };

        TestCreateLogicalStatistics(rowGroupValues, expectedMin, expectedMax);
    }

    [Test]
    public void TestCreateShortStatistics()
    {
        var rowGroupValues = new[]
        {
            new short?[] { -2, -4, 5, null, 1, 0 },
            new short?[] { -6, 7, 4 },
            new short?[] { 2, null, 3, 2 },
            new short?[] { short.MaxValue, short.MinValue },
        };
        var expectedMin = new short[] { -4, -6, 2, short.MinValue };
        var expectedMax = new short[] { 5, 7, 3, short.MaxValue };

        TestCreateLogicalStatistics(rowGroupValues, expectedMin, expectedMax);
    }

    [Test]
    public void TestCreateIntStatistics()
    {
        var rowGroupValues = new[]
        {
            new int?[] { -2, -4, 5, null, 1, 0 },
            new int?[] { -6, 7, 4 },
            new int?[] { 2, null, 3, 2 },
            new int?[] { int.MaxValue, int.MinValue },
        };
        var expectedMin = new[] { -4, -6, 2, int.MinValue };
        var expectedMax = new[] { 5, 7, 3, int.MaxValue };

        TestCreateLogicalStatistics(rowGroupValues, expectedMin, expectedMax);
    }

    [Test]
    public void TestCreateLongStatistics()
    {
        var rowGroupValues = new[]
        {
            new long?[] { -2, -4, 5, null, 1, 0 },
            new long?[] { -6, 7, 4 },
            new long?[] { 2, null, 3, 2 },
            new long?[] { long.MaxValue, long.MinValue },
        };
        var expectedMin = new[] { -4, -6, 2, long.MinValue };
        var expectedMax = new[] { 5, 7, 3, long.MaxValue };

        TestCreateLogicalStatistics(rowGroupValues, expectedMin, expectedMax);
    }

    [Test]
    public void TestCreateByteStatistics()
    {
        var rowGroupValues = new[]
        {
            new byte?[] { 5, null, 1, 0 },
            new byte?[] { 2, null, 2 },
            new byte?[] { byte.MaxValue, byte.MaxValue - 1 },
        };
        var expectedMin = new byte[] { 0, 2, byte.MaxValue - 1 };
        var expectedMax = new byte[] { 5, 2, byte.MaxValue };

        TestCreateLogicalStatistics(rowGroupValues, expectedMin, expectedMax);
    }

    [Test]
    public void TestCreateUShortStatistics()
    {
        var rowGroupValues = new[]
        {
            new ushort?[] { 5, null, 1, 0 },
            new ushort?[] { 2, null, 2 },
            new ushort?[] { ushort.MaxValue, ushort.MaxValue - 1 },
        };
        var expectedMin = new ushort[] { 0, 2, ushort.MaxValue - 1 };
        var expectedMax = new ushort[] { 5, 2, ushort.MaxValue };

        TestCreateLogicalStatistics(rowGroupValues, expectedMin, expectedMax);
    }

    [Test]
    public void TestCreateUIntStatistics()
    {
        var rowGroupValues = new[]
        {
            new uint?[] { 5, null, 1, 0 },
            new uint?[] { 2, null, 2 },
            new uint?[] { uint.MaxValue, uint.MaxValue - 1 },
        };
        var expectedMin = new uint[] { 0, 2, uint.MaxValue - 1 };
        var expectedMax = new uint[] { 5, 2, uint.MaxValue };

        TestCreateLogicalStatistics(rowGroupValues, expectedMin, expectedMax);
    }

    [Test]
    public void TestCreateULongStatistics()
    {
        var rowGroupValues = new[]
        {
            new ulong?[] { 5, null, 1, 0 },
            new ulong?[] { 2, null, 2 },
            new ulong?[] { ulong.MaxValue, ulong.MaxValue - 1 },
        };
        var expectedMin = new ulong[] { 0, 2, ulong.MaxValue - 1 };
        var expectedMax = new ulong[] { 5, 2, ulong.MaxValue };

        TestCreateLogicalStatistics(rowGroupValues, expectedMin, expectedMax);
    }

    [Test]
    public void TestCreateHalfStatistics()
    {
        var rowGroupValues = new[]
        {
            new float?[] { -2.0f, -4.25f, 5.75f, null, float.NaN, 1, 0 }
                .Select(f => f.HasValue ? (Half)f.Value : (Half?)null).ToArray(),
            new Half?[] { Half.MinValue, null, Half.MaxValue },
        };
        var expectedMin = new[] { (Half)(-4.25f), Half.MinValue };
        var expectedMax = new[] { (Half)5.75f, Half.MaxValue };

        TestCreateLogicalStatistics(rowGroupValues, expectedMin, expectedMax);
    }

    [Test]
    public void TestCreateFloatStatistics()
    {
        var rowGroupValues = new[]
        {
            new float?[] { -2.0f, -4.25f, 5.75f, null, float.NaN, 1, 0 },
            new float?[] { float.MinValue, null, float.MaxValue },
        };
        var expectedMin = new[] { -4.25f, float.MinValue };
        var expectedMax = new[] { 5.75f, float.MaxValue };

        TestCreateLogicalStatistics(rowGroupValues, expectedMin, expectedMax);
    }

    [Test]
    public void TestCreateDoubleStatistics()
    {
        var rowGroupValues = new[]
        {
            new double?[] { -2.0, -4.25, 5.75, null, double.NaN, 1, 0 },
            new double?[] { double.MinValue, null, double.MaxValue },
        };
        var expectedMin = new[] { -4.25, double.MinValue };
        var expectedMax = new[] { 5.75, double.MaxValue };

        TestCreateLogicalStatistics(rowGroupValues, expectedMin, expectedMax);
    }

    [Test]
    public void TestAllNullValues()
    {
        using var tmpDir = new DisposableDirectory();
        var filePath = tmpDir.AbsPath("test.parquet");

        var rowGroupValues = new[]
        {
            new int?[] { null, null, null },
        };

        WriteParquet(filePath, rowGroupValues);
        var statistics = GetStatistics(filePath);

        // HasMinMax should be false, so statistics will be null.
        // In future we might want to allow statistics without Min/Max values though,
        // if we want to expose other stats like the null count.
        Assert.That(statistics.Length, Is.EqualTo(1));
        Assert.That(statistics[0], Is.Null);
    }

    [Test]
    public void TestCreateStringStatistics()
    {
        using var tmpDir = new DisposableDirectory();
        var filePath = tmpDir.AbsPath("test.parquet");

        var rowGroupValues = new[]
        {
            new[] { "abc", "def", "ghi" }
        };

        WriteParquet(filePath, rowGroupValues);
        var statistics = GetStatistics(filePath);

        // String statistics are not currently supported
        Assert.That(statistics.Length, Is.EqualTo(1));
        Assert.That(statistics[0], Is.Null);
    }

    [Test]
    public void TestCreateDateStatistics()
    {
        var rowGroupValues = new[]
        {
            new DateOnly?[] { new(2024, 4, 1), new(2024, 4, 2), new(2024, 4, 3) },
            new DateOnly?[] { DateOnly.MinValue, DateOnly.MaxValue },
        };
        var expectedMin = new DateOnly[] { new(2024, 4, 1), DateOnly.MinValue };
        var expectedMax = new DateOnly[] { new(2024, 4, 3), DateOnly.MaxValue };

        TestCreateLogicalStatistics(rowGroupValues, expectedMin, expectedMax);
    }

    [Test]
    public void TestCreateTimespanStatistics()
    {
        using var tmpDir = new DisposableDirectory();
        var filePath = tmpDir.AbsPath("test.parquet");

        var rowGroupValues = new[]
        {
            new TimeSpan[] { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2) }
        };

        WriteParquet(filePath, rowGroupValues);
        var statistics = GetStatistics(filePath);

        // Stats should be null, as although the physical type is int64,
        // we don't currently support creation of logical statistics for this type.
        Assert.That(statistics.Length, Is.EqualTo(1));
        Assert.That(statistics[0], Is.Null);
    }

    private static void TestCreateLogicalStatistics<T>(T?[][] rowGroupValues, T[] expectedMin, T[] expectedMax)
        where T : struct
    {
        using var tmpDir = new DisposableDirectory();
        var filePath = tmpDir.AbsPath("test.parquet");

        WriteParquet(filePath, rowGroupValues);
        var statistics = GetStatistics(filePath);

        for (var rowGroup = 0; rowGroup < rowGroupValues.Length; ++rowGroup)
        {
            var rowGroupStats = statistics[rowGroup];
            Assert.That(rowGroupStats, Is.InstanceOf<LogicalStatistics<T>>());
            var typedStatistics = rowGroupStats as LogicalStatistics<T>;

            Assert.That(typedStatistics!.Min, Is.EqualTo(expectedMin[rowGroup]));
            Assert.That(typedStatistics.Max, Is.EqualTo(expectedMax[rowGroup]));
        }
    }

    private static LogicalStatistics?[] GetStatistics(string filePath)
    {
        using var fileReader = new ParquetFileReader(filePath);
        using var fileMetadata = fileReader.FileMetaData;
        var columnDescriptor = fileMetadata.Schema.Column(0);

        var stats = new LogicalStatistics?[fileMetadata.NumRowGroups];

        for (var rowGroup = 0; rowGroup < fileMetadata.NumRowGroups; ++rowGroup)
        {
            using var rowGroupReader = fileReader.RowGroup(rowGroup);

            using var columnMetadata = rowGroupReader.MetaData.GetColumnChunkMetaData(0);
            using var statistics = columnMetadata.Statistics;
            Assert.That(statistics, Is.Not.Null);

            stats[rowGroup] = LogicalStatistics.FromStatistics(statistics!, columnDescriptor);
        }

        return stats;
    }

    private static void WriteParquet<T>(string path, T[][] rowGroupValues)
    {
        var columns = new Column[]
        {
            new Column<T>("x")
        };
        using var writer = new ParquetFileWriter(path, columns);
        foreach (var values in rowGroupValues)
        {
            using var rowGroupWriter = writer.AppendRowGroup();
            using var colWriter = rowGroupWriter.NextColumn().LogicalWriter<T>();
            colWriter.WriteBatch(values);
        }

        writer.Close();
    }
}
