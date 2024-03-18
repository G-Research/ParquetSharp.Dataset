namespace ParquetSharp.Dataset.Filter;

internal sealed class IntRangeStatisticsEvaluator : BaseStatisticsEvaluator
{
    public IntRangeStatisticsEvaluator(long start, long end)
    {
        _start = start;
        _end = end;
    }

    protected override bool IncludeRowGroup(LogicalStatistics<byte> stats)
    {
        return _end >= stats.Min && _start <= stats.Max;
    }

    protected override bool IncludeRowGroup(LogicalStatistics<ushort> stats)
    {
        return _end >= stats.Min && _start <= stats.Max;
    }

    protected override bool IncludeRowGroup(LogicalStatistics<uint> stats)
    {
        return _end >= stats.Min && _start <= stats.Max;
    }

    protected override bool IncludeRowGroup(LogicalStatistics<ulong> stats)
    {
        return (_end >= 0 && (ulong)_end >= stats.Min) && (_start < 0 || (ulong)_start <= stats.Max);
    }

    protected override bool IncludeRowGroup(LogicalStatistics<sbyte> stats)
    {
        return _end >= stats.Min && _start <= stats.Max;
    }

    protected override bool IncludeRowGroup(LogicalStatistics<short> stats)
    {
        return _end >= stats.Min && _start <= stats.Max;
    }

    protected override bool IncludeRowGroup(LogicalStatistics<int> stats)
    {
        return _end >= stats.Min && _start <= stats.Max;
    }

    protected override bool IncludeRowGroup(LogicalStatistics<long> stats)
    {
        return _end >= stats.Min && _start <= stats.Max;
    }

    private readonly long _start;
    private readonly long _end;
}
