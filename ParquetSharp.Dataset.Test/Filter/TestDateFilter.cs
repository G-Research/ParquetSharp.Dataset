using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;
using NUnit.Framework;
using ParquetSharp.Dataset.Filter;

namespace ParquetSharp.Dataset.Test.Filter;

[TestFixture(typeof(Date32Array), typeof(int), typeof(Date32Array.Builder))]
[TestFixture(typeof(Date64Array), typeof(long), typeof(Date64Array.Builder))]
public class TestDateFilter<TArray, TUnderlying, TBuilder>
    where TArray : IArrowArray
    where TBuilder : DateArrayBuilder<TUnderlying, TArray, TBuilder>, new()
{
    [Test]
    public void TestComputeMask()
    {
        var rangeStart = new DateOnly(2024, 2, 1);
        var rangeEnd = new DateOnly(2024, 2, 10);
        var filter = Col.Named("date").IsInRange(rangeStart, rangeEnd);

        var dateValues = Enumerable.Range(0, 100)
            .Select(i => new DateOnly(2024, 1, 1).AddDays(i))
            .ToArray();
        var dateArray = new TBuilder()
            .AppendNull()
            .AppendRange(dateValues)
            .Build();

        var recordBatch = new RecordBatch.Builder()
            .Append("date", true, dateArray)
            .Build();

        var mask = filter.ComputeMask(recordBatch);

        Assert.That(mask, Is.Not.Null);
        Assert.That(mask!.IncludedCount, Is.EqualTo(10));
        for (var i = 0; i < dateValues.Length; ++i)
        {
            var expectIncluded = i >= 32 && i <= 41;
            Assert.That(BitUtility.GetBit(mask.Mask.Span, i), Is.EqualTo(expectIncluded));
        }
    }
}

[TestFixture]
public class TestDateFilter
{
    [Test]
    public void TestComputeMaskWithInvalidColumnType()
    {
        var filter = Col.Named("date").IsInRange(new DateOnly(2024, 2, 1), new DateOnly(2024, 2, 10));

        var dateArray = new Int32Array.Builder()
            .AppendRange(Enumerable.Range(0, 100))
            .Build();
        var recordBatch = new RecordBatch.Builder()
            .Append("date", true, dateArray)
            .Build();

        var exception = Assert.Throws<NotSupportedException>(() => filter.ComputeMask(recordBatch));
        Assert.That(exception!.Message, Is.EqualTo(
            "Date range filter for column 'date' does not support arrays with type int32"));
    }

    [Test]
    public void TestIncludeRowGroup()
    {
        var filter = Col.Named("date").IsInRange(new DateOnly(2024, 2, 1), new DateOnly(2024, 2, 10));

        foreach (var (min, max, expectInclude) in new[]
                 {
                     (new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 31), false),
                     (new DateOnly(2024, 1, 1), new DateOnly(2024, 2, 1), true),
                     (new DateOnly(2024, 2, 10), new DateOnly(2024, 2, 11), true),
                     (new DateOnly(2024, 2, 11), new DateOnly(2024, 2, 12), false),
                     (new DateOnly(2024, 1, 1), new DateOnly(2024, 2, 29), true),
                     (new DateOnly(2024, 2, 4), new DateOnly(2024, 2, 4), true),
                 })
        {
            var statistics = new Dictionary<string, LogicalStatistics>
            {
                { "date", new LogicalStatistics<DateOnly>(min, max) }
            };

            var includeRowGroup = filter.IncludeRowGroup(statistics);

            Assert.That(includeRowGroup, Is.EqualTo(expectInclude));
        }
    }
}
