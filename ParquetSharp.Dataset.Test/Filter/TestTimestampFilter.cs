using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;
using NUnit.Framework;
using ParquetSharp.Dataset.Filter;

namespace ParquetSharp.Dataset.Test.Filter;

using TimeUnit = Apache.Arrow.Types.TimeUnit;

[TestFixture]
public class TestTimestampFilter
{
    [Test]
    public void TestComputeRangeMask([Values] TimeUnit unit)
    {
        var rangeStart = new DateTime(2024, 8, 21, 14, 12, 0);
        var rangeEnd = new DateTime(2024, 8, 21, 14, 17, 0);
        var filter = Col.Named("timestamp").IsInRange(rangeStart, rangeEnd);

        var timeValues = Enumerable.Range(0, 100)
            .Select(i => new DateTimeOffset(2024, 8, 21, 14, 0, 0, TimeSpan.Zero).AddMinutes(i))
            .ToArray();
        var timestampArray = new TimestampArray.Builder(unit)
            .AppendNull()
            .AppendRange(timeValues)
            .Build();

        var recordBatch = new RecordBatch.Builder()
            .Append("timestamp", true, timestampArray)
            .Build();

        var mask = filter.ComputeMask(recordBatch);

        Assert.That(mask, Is.Not.Null);
        Assert.That(mask!.IncludedCount, Is.EqualTo(5));
        for (var i = 0; i < timeValues.Length; ++i)
        {
            var expectIncluded = i >= 13 && i < 18;
            Assert.That(BitUtility.GetBit(mask.Mask.Span, i), Is.EqualTo(expectIncluded));
        }
    }

    [Test]
    public void TestComputeEqMask([Values] TimeUnit unit, [Values] bool includeNull)
    {
        var value = new DateTime(2025, 5, 19, 12, 34, 0);
        var filter = Col.Named("timestamp").IsEqualTo(value);
        TestComputeComparisonMask(unit, filter, dt => dt == value, includeNull);
    }

    [Test]
    public void TestComputeGtMask([Values] TimeUnit unit, [Values] bool includeNull)
    {
        var value = new DateTime(2025, 5, 19, 12, 34, 0);
        var filter = Col.Named("timestamp").IsGreaterThan(value);
        TestComputeComparisonMask(unit, filter, dt => dt > value, includeNull);
    }

    [Test]
    public void TestComputeGtEqMask([Values] TimeUnit unit, [Values] bool includeNull)
    {
        var value = new DateTime(2025, 5, 19, 12, 34, 0);
        var filter = Col.Named("timestamp").IsGreaterThanOrEqual(value);
        TestComputeComparisonMask(unit, filter, dt => dt >= value, includeNull);
    }

    [Test]
    public void TestComputeLtMask([Values] TimeUnit unit, [Values] bool includeNull)
    {
        var value = new DateTime(2025, 5, 19, 12, 34, 0);
        var filter = Col.Named("timestamp").IsLessThan(value);
        TestComputeComparisonMask(unit, filter, dt => dt < value, includeNull);
    }

    [Test]
    public void TestComputeLtEqMask([Values] TimeUnit unit, [Values] bool includeNull)
    {
        var value = new DateTime(2025, 5, 19, 12, 34, 0);
        var filter = Col.Named("timestamp").IsLessThanOrEqual(value);
        TestComputeComparisonMask(unit, filter, dt => dt <= value, includeNull);
    }

    private static void TestComputeComparisonMask(TimeUnit unit, IFilter filter, Func<DateTime, bool> predicate, bool includeNull)
    {
        var timeValues = Enumerable.Range(0, 100)
            .Select(i => new DateTimeOffset(2025, 5, 19, 12, 0, 0, TimeSpan.Zero).AddMinutes(i))
            .ToArray();
        var timestampArrayBuilder = new TimestampArray.Builder(unit);
        if (includeNull)
        {
            timestampArrayBuilder.AppendNull();
        }

        var timestampArray = timestampArrayBuilder
            .AppendRange(timeValues)
            .Build();

        var recordBatch = new RecordBatch.Builder()
            .Append("timestamp", includeNull, timestampArray)
            .Build();

        var mask = filter.ComputeMask(recordBatch);

        var expectedMask = new List<bool>();
        if (includeNull)
        {
            expectedMask.Add(false);
        }

        foreach (var value in timeValues)
        {
            expectedMask.Add(predicate(value.DateTime));
        }

        Assert.That(mask, Is.Not.Null);
        Assert.That(mask!.IncludedCount, Is.GreaterThan(0));
        Assert.That(mask.IncludedCount, Is.LessThan(timeValues.Length));
        for (var i = 0; i < expectedMask.Count; ++i)
        {
            Assert.That(BitUtility.GetBit(mask.Mask.Span, i), Is.EqualTo(expectedMask[i]));
        }
    }

    [Test]
    public void TestComputeMaskWithInvalidColumnType()
    {
        var rangeStart = new DateTime(2024, 8, 21, 14, 12, 0);
        var rangeEnd = new DateTime(2024, 8, 21, 14, 17, 0);

        var timestampArray = new Int32Array.Builder()
            .AppendRange(Enumerable.Range(0, 100))
            .Build();
        var recordBatch = new RecordBatch.Builder()
            .Append("timestamp", true, timestampArray)
            .Build();

        var filter = Col.Named("timestamp").IsInRange(rangeStart, rangeEnd);
        var exception = Assert.Throws<NotSupportedException>(() => filter.ComputeMask(recordBatch));
        Assert.That(exception!.Message, Is.EqualTo(
            "Timestamp range filter for column 'timestamp' does not support arrays with type int32"));

        filter = Col.Named("timestamp").IsEqualTo(rangeStart);
        exception = Assert.Throws<NotSupportedException>(() => filter.ComputeMask(recordBatch));
        Assert.That(exception!.Message, Is.EqualTo(
            "Timestamp comparison filter for column 'timestamp' does not support arrays with type int32"));
    }

    [Test]
    public void TestTimestampRangeIncludeRowGroup([Values] bool nanoseconds)
    {
        var rangeStart = new DateTime(2024, 8, 21, 14, 12, 0);
        var rangeEnd = new DateTime(2024, 8, 21, 14, 17, 0);
        var filter = Col.Named("timestamp").IsInRange(rangeStart, rangeEnd);

        foreach (var (minMins, minSecs, maxMins, maxSecs, expectInclude) in new[]
                 {
                     (11, 12, 11, 59, false),
                     (11, 12, 12, 0, true),
                     (11, 12, 13, 0, true),
                     (14, 0, 15, 0, true),
                     (15, 0, 18, 0, true),
                     (17, 0, 17, 0, false),
                     (18, 0, 19, 0, false),
                 })
        {
            var min = new DateTime(2024, 8, 21, 14, minMins, minSecs);
            var max = new DateTime(2024, 8, 21, 14, maxMins, maxSecs);
            var stats = nanoseconds
                ? (LogicalStatistics)new LogicalStatistics<DateTimeNanos>(new DateTimeNanos(min), new DateTimeNanos(max))
                : new LogicalStatistics<DateTime>(min, max);
            var statistics = new Dictionary<string, LogicalStatistics>
            {
                { "timestamp", stats }
            };

            var includeRowGroup = filter.IncludeRowGroup(statistics);

            Assert.That(includeRowGroup, Is.EqualTo(expectInclude));
        }
    }

    [Test]
    public void TestTimestampComparisonIncludeRowGroup([Values] bool nanoseconds)
    {
        var statisticsRanges = new[]
        {
            (new DateTime(2025, 1, 1, 9, 0, 0), new DateTime(2025, 1, 31, 17, 0, 0)),
            (new DateTime(2025, 2, 1, 9, 0, 0), new DateTime(2025, 2, 28, 17, 0, 0)),
            (new DateTime(2025, 3, 1, 9, 0, 0), new DateTime(2025, 3, 31, 17, 0, 0)),
        };

        Dictionary<string, LogicalStatistics>[] statistics;
        if (nanoseconds)
        {
            statistics = statisticsRanges.Select(minMax => new Dictionary<string, LogicalStatistics>
            {
                {
                    "timestamp", new LogicalStatistics<DateTimeNanos>(
                        new DateTimeNanos(minMax.Item1), new DateTimeNanos(minMax.Item2))
                }
            }).ToArray();
        }
        else
        {
            statistics = statisticsRanges.Select(minMax => new Dictionary<string, LogicalStatistics>
            {
                { "timestamp", new LogicalStatistics<DateTime>(minMax.Item1, minMax.Item2) }
            }).ToArray();
        }

        {
            var filter = Col.Named("timestamp").IsEqualTo(new DateTime(2025, 2, 2, 12, 0, 0));
            var includedGroups = statistics.Select(s => filter.IncludeRowGroup(s)).ToArray();
            Assert.That(includedGroups, Is.EqualTo(new[] { false, true, false }));
        }

        {
            var filter = Col.Named("timestamp").IsGreaterThan(new DateTime(2025, 1, 31, 17, 0, 0));
            var includedGroups = statistics.Select(s => filter.IncludeRowGroup(s)).ToArray();
            Assert.That(includedGroups, Is.EqualTo(new[] { false, true, true }));
        }

        {
            var filter = Col.Named("timestamp").IsGreaterThanOrEqual(new DateTime(2025, 2, 28, 17, 0, 0));
            var includedGroups = statistics.Select(s => filter.IncludeRowGroup(s)).ToArray();
            Assert.That(includedGroups, Is.EqualTo(new[] { false, true, true }));
        }

        {
            var filter = Col.Named("timestamp").IsLessThan(new DateTime(2025, 3, 1, 9, 0, 0));
            var includedGroups = statistics.Select(s => filter.IncludeRowGroup(s)).ToArray();
            Assert.That(includedGroups, Is.EqualTo(new[] { true, true, false }));
        }

        {
            var filter = Col.Named("timestamp").IsLessThanOrEqual(new DateTime(2025, 2, 1, 9, 0, 0));
            var includedGroups = statistics.Select(s => filter.IncludeRowGroup(s)).ToArray();
            Assert.That(includedGroups, Is.EqualTo(new[] { true, true, false }));
        }
    }
}
