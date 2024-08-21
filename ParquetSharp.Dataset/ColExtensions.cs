using System;
using System.Collections.Generic;

namespace ParquetSharp.Dataset;

using ParquetSharp.Dataset.Filter;

public static class ColExtensions
{
    /// <summary>
    /// Filter based on an integer typed column being equal to a specified value
    /// </summary>
    /// <param name="column">The column to add the condition on</param>
    /// <param name="value">The value to filter on</param>
    /// <returns>Created filter</returns>
    public static IFilter IsEqualTo(this Col column, long value)
    {
        return new ColumnValueFilter(
            column.Name, new IntEqualityEvaluator(value, column.Name), new IntEqualityStatisticsEvaluator(value));
    }

    /// <summary>
    /// Filter based on an integer typed column being within a specified range
    /// </summary>
    /// <param name="column">The column to add the condition on</param>
    /// <param name="start">The first value of the range (inclusive)</param>
    /// <param name="end">The last value of the range (inclusive)</param>
    /// <returns>Created filter</returns>
    public static IFilter IsInRange(this Col column, long start, long end)
    {
        return new ColumnValueFilter(
            column.Name, new IntRangeEvaluator(start, end, column.Name), new IntRangeStatisticsEvaluator(start, end));
    }

    /// <summary>
    /// Filter based on a string typed column being equal to a specified value
    /// </summary>
    /// <param name="column">The column to add the condition on</param>
    /// <param name="value">The value to filter on</param>
    /// <returns>Created filter</returns>
    public static IFilter IsEqualTo(this Col column, string? value)
    {
        return new ColumnValueFilter(column.Name, new StringInSetEvaluator(new[] { value }, column.Name));
    }

    /// <summary>
    /// Filter based on a string typed column being within a set of specified values
    /// </summary>
    /// <param name="column">The column to add the condition on</param>
    /// <param name="values">The values to filter on</param>
    /// <returns>Created filter</returns>
    public static IFilter IsIn(this Col column, IReadOnlyList<string?> values)
    {
        return new ColumnValueFilter(column.Name, new StringInSetEvaluator(values, column.Name));
    }

    /// <summary>
    /// Filter based on a date typed column being within a specified date range
    /// </summary>
    /// <param name="column">The date column to add the condition on</param>
    /// <param name="start">The first date of the range (inclusive)</param>
    /// <param name="end">The last date of the range (inclusive)</param>
    /// <returns>Created filter</returns>
    public static IFilter IsInRange(this Col column, DateOnly start, DateOnly end)
    {
        return new ColumnValueFilter(
            column.Name, new DateRangeEvaluator(start, end, column.Name), new DateRangeStatisticsEvaluator(start, end));
    }

    /// <summary>
    /// Filter based on a Timestamp typed column being within a specified range.
    /// Time zones are not accounted for, the start and end times must be in the same time zone as the column data.
    /// </summary>
    /// <param name="column">The Timestamp column to add the condition on</param>
    /// <param name="start">The start time of the range (inclusive)</param>
    /// <param name="end">The end time of the range (exclusive)</param>
    /// <returns>Created filter</returns>
    public static IFilter IsInRange(this Col column, DateTime start, DateTime end)
    {
        return new ColumnValueFilter(
            column.Name,
            new TimestampRangeEvaluator(start, end, column.Name), new TimestampRangeStatisticsEvaluator(start, end));
    }
}
