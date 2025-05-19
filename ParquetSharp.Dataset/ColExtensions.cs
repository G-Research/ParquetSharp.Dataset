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
            column.Name,
            new IntComparisonEvaluator(ComparisonOperator.Equal, value, column.Name),
            new IntComparisonStatisticsEvaluator(ComparisonOperator.Equal, value));
    }

    /// <summary>
    /// Filter based on an integer typed column being greater than a specified value
    /// </summary>
    /// <param name="column">The column to add the condition on</param>
    /// <param name="value">The value to compare to</param>
    /// <returns>Created filter</returns>
    public static IFilter IsGreaterThan(this Col column, long value)
    {
        return new ColumnValueFilter(
            column.Name,
            new IntComparisonEvaluator(ComparisonOperator.GreaterThan, value, column.Name),
            new IntComparisonStatisticsEvaluator(ComparisonOperator.GreaterThan, value));
    }

    /// <summary>
    /// Filter based on an integer typed column being greater than or equal to a specified value
    /// </summary>
    /// <param name="column">The column to add the condition on</param>
    /// <param name="value">The value to compare to</param>
    /// <returns>Created filter</returns>
    public static IFilter IsGreaterThanOrEqual(this Col column, long value)
    {
        return new ColumnValueFilter(
            column.Name,
            new IntComparisonEvaluator(ComparisonOperator.GreaterThanOrEqual, value, column.Name),
            new IntComparisonStatisticsEvaluator(ComparisonOperator.GreaterThanOrEqual, value));
    }

    /// <summary>
    /// Filter based on an integer typed column being less than a specified value
    /// </summary>
    /// <param name="column">The column to add the condition on</param>
    /// <param name="value">The value to compare to</param>
    /// <returns>Created filter</returns>
    public static IFilter IsLessThan(this Col column, long value)
    {
        return new ColumnValueFilter(
            column.Name,
            new IntComparisonEvaluator(ComparisonOperator.LessThan, value, column.Name),
            new IntComparisonStatisticsEvaluator(ComparisonOperator.LessThan, value));
    }

    /// <summary>
    /// Filter based on an integer typed column being less than or equal to a specified value
    /// </summary>
    /// <param name="column">The column to add the condition on</param>
    /// <param name="value">The value to compare to</param>
    /// <returns>Created filter</returns>
    public static IFilter IsLessThanOrEqual(this Col column, long value)
    {
        return new ColumnValueFilter(
            column.Name,
            new IntComparisonEvaluator(ComparisonOperator.LessThanOrEqual, value, column.Name),
            new IntComparisonStatisticsEvaluator(ComparisonOperator.LessThanOrEqual, value));
    }

    /// <summary>
    /// Filter based on an integer typed column being within a specified range
    /// </summary>
    /// <param name="column">The column to add the condition on</param>
    /// <param name="start">The start value of the range (inclusive)</param>
    /// <param name="end">The end value of the range (exclusive)</param>
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
    /// <param name="start">The start date of the range (inclusive)</param>
    /// <param name="end">The end date of the range (exclusive)</param>
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
