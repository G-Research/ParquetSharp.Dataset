namespace ParquetSharp.Dataset.Filter;

internal sealed class IntEqualityStatisticsEvaluator
    : ILogicalStatisticsVisitor<byte, bool>,
        ILogicalStatisticsVisitor<ushort, bool>,
        ILogicalStatisticsVisitor<uint, bool>,
        ILogicalStatisticsVisitor<ulong, bool>,
        ILogicalStatisticsVisitor<sbyte, bool>,
        ILogicalStatisticsVisitor<short, bool>,
        ILogicalStatisticsVisitor<int, bool>,
        ILogicalStatisticsVisitor<long, bool>
{
    public IntEqualityStatisticsEvaluator(long value)
    {
        _value = value;
    }

    public bool Visit(LogicalStatistics stats)
    {
        return true;
    }

    public bool Visit(LogicalStatistics<byte> stats)
    {
        return _value >= stats.Min && _value <= stats.Max;
    }

    public bool Visit(LogicalStatistics<ushort> stats)
    {
        return _value >= stats.Min && _value <= stats.Max;
    }

    public bool Visit(LogicalStatistics<uint> stats)
    {
        return _value >= stats.Min && _value <= stats.Max;
    }

    public bool Visit(LogicalStatistics<ulong> stats)
    {
        return _value >= 0 && (ulong)_value >= stats.Min && (ulong)_value <= stats.Max;
    }

    public bool Visit(LogicalStatistics<sbyte> stats)
    {
        return _value >= stats.Min && _value <= stats.Max;
    }

    public bool Visit(LogicalStatistics<short> stats)
    {
        return _value >= stats.Min && _value <= stats.Max;
    }

    public bool Visit(LogicalStatistics<int> stats)
    {
        return _value >= stats.Min && _value <= stats.Max;
    }

    public bool Visit(LogicalStatistics<long> stats)
    {
        return _value >= stats.Min && _value <= stats.Max;
    }

    private readonly long _value;
}
