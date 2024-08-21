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
    [TestCase(TimeUnit.Second)]
    [TestCase(TimeUnit.Millisecond)]
    [TestCase(TimeUnit.Microsecond)]
    [TestCase(TimeUnit.Nanosecond)]
    public void TestComputeMask(TimeUnit unit)
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
    public void TestComputeMaskWithInvalidColumnType()
    {
        var rangeStart = new DateTime(2024, 8, 21, 14, 12, 0);
        var rangeEnd = new DateTime(2024, 8, 21, 14, 17, 0);
        var filter = Col.Named("timestamp").IsInRange(rangeStart, rangeEnd);

        var timestampArray = new Int32Array.Builder()
            .AppendRange(Enumerable.Range(0, 100))
            .Build();
        var recordBatch = new RecordBatch.Builder()
            .Append("timestamp", true, timestampArray)
            .Build();

        var exception = Assert.Throws<NotSupportedException>(() => filter.ComputeMask(recordBatch));
        Assert.That(exception!.Message, Is.EqualTo(
            "Timestamp range filter for column 'timestamp' does not support arrays with type int32"));
    }

    [Test]
    public void TestIncludeRowGroup([Values] bool nanoseconds)
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
}
