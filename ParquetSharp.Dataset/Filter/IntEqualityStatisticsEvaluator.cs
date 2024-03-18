namespace ParquetSharp.Dataset.Filter;

internal sealed class IntEqualityStatisticsEvaluator : BaseStatisticsEvaluator
{
    public IntEqualityStatisticsEvaluator(long value)
    {
        _value = value;
    }

    protected override bool IncludeRowGroup(LogicalStatistics<byte> stats)
    {
        return _value >= stats.Min && _value <= stats.Max;
    }

    protected override bool IncludeRowGroup(LogicalStatistics<ushort> stats)
    {
        return _value >= stats.Min && _value <= stats.Max;
    }

    protected override bool IncludeRowGroup(LogicalStatistics<uint> stats)
    {
        return _value >= stats.Min && _value <= stats.Max;
    }

    protected override bool IncludeRowGroup(LogicalStatistics<ulong> stats)
    {
        return _value >= 0 && (ulong)_value >= stats.Min && (ulong)_value <= stats.Max;
    }

    protected override bool IncludeRowGroup(LogicalStatistics<sbyte> stats)
    {
        return _value >= stats.Min && _value <= stats.Max;
    }

    protected override bool IncludeRowGroup(LogicalStatistics<short> stats)
    {
        return _value >= stats.Min && _value <= stats.Max;
    }

    protected override bool IncludeRowGroup(LogicalStatistics<int> stats)
    {
        return _value >= stats.Min && _value <= stats.Max;
    }

    protected override bool IncludeRowGroup(LogicalStatistics<long> stats)
    {
        return _value >= stats.Min && _value <= stats.Max;
    }

    private readonly long _value;
}
