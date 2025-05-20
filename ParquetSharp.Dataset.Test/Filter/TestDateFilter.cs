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
    public void TestDateRangeComputeMask()
    {
        var rangeStart = new DateOnly(2024, 2, 1);
        var rangeEnd = new DateOnly(2024, 2, 11);
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

    [Test]
    public void TestDateEqComputeMask([Values] bool includeNull)
    {
        TestDateComparisonComputeMask(
            Col.Named("date").IsEqualTo(new DateOnly(2024, 2, 1)),
            date => date == new DateOnly(2024, 2, 1),
            includeNull);
    }

    [Test]
    public void TestDateGtComputeMask([Values] bool includeNull)
    {
        TestDateComparisonComputeMask(
            Col.Named("date").IsGreaterThan(new DateOnly(2024, 2, 1)),
            date => date > new DateOnly(2024, 2, 1),
            includeNull);
    }

    [Test]
    public void TestDateGtEqComputeMask([Values] bool includeNull)
    {
        TestDateComparisonComputeMask(
            Col.Named("date").IsGreaterThanOrEqual(new DateOnly(2024, 2, 1)),
            date => date >= new DateOnly(2024, 2, 1),
            includeNull);
    }

    [Test]
    public void TestDateLtComputeMask([Values] bool includeNull)
    {
        TestDateComparisonComputeMask(
            Col.Named("date").IsLessThan(new DateOnly(2024, 2, 1)),
            date => date < new DateOnly(2024, 2, 1),
            includeNull);
    }

    [Test]
    public void TestDateLtEqComputeMask([Values] bool includeNull)
    {
        TestDateComparisonComputeMask(
            Col.Named("date").IsLessThanOrEqual(new DateOnly(2024, 2, 1)),
            date => date <= new DateOnly(2024, 2, 1),
            includeNull);
    }

    private static void TestDateComparisonComputeMask(IFilter filter, Func<DateOnly, bool> expectIncluded, bool includeNull)
    {
        var dateValues = Enumerable.Range(0, 100)
            .Select(i => new DateOnly(2024, 1, 1).AddDays(i))
            .ToArray();
        var dateArrayBuilder = new TBuilder();
        if (includeNull)
        {
            dateArrayBuilder.AppendNull();
        }

        var dateArray = dateArrayBuilder
            .AppendRange(dateValues)
            .Build();

        var recordBatch = new RecordBatch.Builder()
            .Append("date", includeNull, dateArray)
            .Build();

        var expectedMask = new List<bool>();
        if (includeNull)
        {
            expectedMask.Add(false);
        }

        foreach (var dateValue in dateValues)
        {
            expectedMask.Add(expectIncluded(dateValue));
        }

        var mask = filter.ComputeMask(recordBatch);

        Assert.That(mask, Is.Not.Null);
        Assert.That(mask.IncludedCount, Is.GreaterThan(0));
        for (var i = 0; i < expectedMask.Count; ++i)
        {
            Assert.That(BitUtility.GetBit(mask.Mask.Span, i), Is.EqualTo(expectedMask[i]));
        }
    }
}

[TestFixture]
public class TestDateFilter
{
    [Test]
    public void TestComputeMaskWithInvalidColumnType()
    {
        var filter = Col.Named("date").IsInRange(new DateOnly(2024, 2, 1), new DateOnly(2024, 2, 11));

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
    public void TestDateRangeIncludeRowGroup()
    {
        var filter = Col.Named("date").IsInRange(new DateOnly(2024, 2, 1), new DateOnly(2024, 2, 11));

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

    [Test]
    public void TestDateComparisonIncludeRowGroup()
    {
        var statisticsRanges = new[]
        {
            (new DateOnly(2025, 1, 1), new DateOnly(2025, 1, 31)),
            (new DateOnly(2025, 2, 1), new DateOnly(2025, 2, 28)),
            (new DateOnly(2025, 3, 1), new DateOnly(2025, 3, 31)),
        };
        var statistics = statisticsRanges.Select(minMax => new Dictionary<string, LogicalStatistics>
        {
            { "date", new LogicalStatistics<DateOnly>(minMax.Item1, minMax.Item2) }
        }).ToArray();

        {
            var filter = Col.Named("date").IsEqualTo(new DateOnly(2025, 2, 2));
            var includedGroups = statistics.Select(s => filter.IncludeRowGroup(s)).ToArray();
            Assert.That(includedGroups, Is.EqualTo(new[] { false, true, false }));
        }

        {
            var filter = Col.Named("date").IsGreaterThan(new DateOnly(2025, 1, 31));
            var includedGroups = statistics.Select(s => filter.IncludeRowGroup(s)).ToArray();
            Assert.That(includedGroups, Is.EqualTo(new[] { false, true, true }));
        }

        {
            var filter = Col.Named("date").IsGreaterThanOrEqual(new DateOnly(2025, 2, 28));
            var includedGroups = statistics.Select(s => filter.IncludeRowGroup(s)).ToArray();
            Assert.That(includedGroups, Is.EqualTo(new[] { false, true, true }));
        }

        {
            var filter = Col.Named("date").IsLessThan(new DateOnly(2025, 3, 1));
            var includedGroups = statistics.Select(s => filter.IncludeRowGroup(s)).ToArray();
            Assert.That(includedGroups, Is.EqualTo(new[] { true, true, false }));
        }

        {
            var filter = Col.Named("date").IsLessThanOrEqual(new DateOnly(2025, 2, 1));
            var includedGroups = statistics.Select(s => filter.IncludeRowGroup(s)).ToArray();
            Assert.That(includedGroups, Is.EqualTo(new[] { true, true, false }));
        }
    }
}
