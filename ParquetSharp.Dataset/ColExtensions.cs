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
        return new ColumnValueFilter(column.Name, new IntEqualityEvaluator(value, column.Name));
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
        return new ColumnValueFilter(column.Name, new IntRangeEvaluator(start, end, column.Name));
    }

    /// <summary>
    /// Filter based on a string typed column being equal to a specified value
    /// </summary>
    /// <param name="column">The column to add the condition on</param>
    /// <param name="value">The value to filter on</param>
    /// <returns>Created filter</returns>
    public static IFilter IsEqualTo(this Col column, string value)
    {
        return new ColumnValueFilter(column.Name, new StringInSetEvaluator(new [] {value}, column.Name));
    }

    /// <summary>
    /// Filter based on a string typed column being within a set of specified values
    /// </summary>
    /// <param name="column">The column to add the condition on</param>
    /// <param name="values">The values to filter on</param>
    /// <returns>Created filter</returns>
    public static IFilter IsIn(this Col column, IReadOnlyList<string> values)
    {
        return new ColumnValueFilter(column.Name, new StringInSetEvaluator(values, column.Name));
    }
}
