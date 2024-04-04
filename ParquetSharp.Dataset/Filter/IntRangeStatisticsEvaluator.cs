namespace ParquetSharp.Dataset.Filter;

internal sealed class IntRangeStatisticsEvaluator
    : ILogicalStatisticsVisitor<byte, bool>,
        ILogicalStatisticsVisitor<ushort, bool>,
        ILogicalStatisticsVisitor<uint, bool>,
        ILogicalStatisticsVisitor<ulong, bool>,
        ILogicalStatisticsVisitor<sbyte, bool>,
        ILogicalStatisticsVisitor<short, bool>,
        ILogicalStatisticsVisitor<int, bool>,
        ILogicalStatisticsVisitor<long, bool>
{
    public IntRangeStatisticsEvaluator(long start, long end)
    {
        _start = start;
        _end = end;
    }

    public bool Visit(LogicalStatistics stats)
    {
        return true;
    }

    public bool Visit(LogicalStatistics<byte> stats)
    {
        return _end >= stats.Min && _start <= stats.Max;
    }

    public bool Visit(LogicalStatistics<ushort> stats)
    {
        return _end >= stats.Min && _start <= stats.Max;
    }

    public bool Visit(LogicalStatistics<uint> stats)
    {
        return _end >= stats.Min && _start <= stats.Max;
    }

    public bool Visit(LogicalStatistics<ulong> stats)
    {
        return (_end >= 0 && (ulong)_end >= stats.Min) && (_start < 0 || (ulong)_start <= stats.Max);
    }

    public bool Visit(LogicalStatistics<sbyte> stats)
    {
        return _end >= stats.Min && _start <= stats.Max;
    }

    public bool Visit(LogicalStatistics<short> stats)
    {
        return _end >= stats.Min && _start <= stats.Max;
    }

    public bool Visit(LogicalStatistics<int> stats)
    {
        return _end >= stats.Min && _start <= stats.Max;
    }

    public bool Visit(LogicalStatistics<long> stats)
    {
        return _end >= stats.Min && _start <= stats.Max;
    }

    private readonly long _start;
    private readonly long _end;
}
