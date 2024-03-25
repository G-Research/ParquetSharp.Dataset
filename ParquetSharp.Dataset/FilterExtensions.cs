using ParquetSharp.Dataset.Filter;

namespace ParquetSharp.Dataset;

public static class FilterExtensions
{
    /// <summary>
    /// Create a filter that is satisfied if both this filter and another are satisfied
    /// </summary>
    /// <param name="filter">The first filter to test</param>
    /// <param name="other">An additional filter to test</param>
    /// <returns>Combined filter</returns>
    public static IFilter And(this IFilter filter, IFilter other)
    {
        return new AndFilter(filter, other);
    }

    /// <summary>
    /// Create a filter that is satisfied if either this filter or another are satisfied
    /// </summary>
    /// <param name="filter">The first filter to test</param>
    /// <param name="other">An additional filter to test</param>
    /// <returns>Combined filter</returns>
    public static IFilter Or(this IFilter filter, IFilter other)
    {
        return new OrFilter(filter, other);
    }
}
