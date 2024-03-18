using System;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Base class for evaluating whether a row group should be read based on column statistics.
/// Defaults to including row groups if handling a type isn't overridden.
/// </summary>
internal class BaseStatisticsEvaluator
{
    public bool IncludeRowGroup(LogicalStatistics untypedStats)
    {
        return untypedStats switch
        {
            LogicalStatistics<bool> stats => IncludeRowGroup(stats),
            LogicalStatistics<byte> stats => IncludeRowGroup(stats),
            LogicalStatistics<ushort> stats => IncludeRowGroup(stats),
            LogicalStatistics<uint> stats => IncludeRowGroup(stats),
            LogicalStatistics<ulong> stats => IncludeRowGroup(stats),
            LogicalStatistics<sbyte> stats => IncludeRowGroup(stats),
            LogicalStatistics<short> stats => IncludeRowGroup(stats),
            LogicalStatistics<int> stats => IncludeRowGroup(stats),
            LogicalStatistics<long> stats => IncludeRowGroup(stats),
            LogicalStatistics<Half> stats => IncludeRowGroup(stats),
            LogicalStatistics<float> stats => IncludeRowGroup(stats),
            LogicalStatistics<double> stats => IncludeRowGroup(stats),
            _ => true
        };
    }

    protected virtual bool IncludeRowGroup(LogicalStatistics<bool> stats) => true;

    protected virtual bool IncludeRowGroup(LogicalStatistics<byte> stats) => true;

    protected virtual bool IncludeRowGroup(LogicalStatistics<ushort> stats) => true;

    protected virtual bool IncludeRowGroup(LogicalStatistics<uint> stats) => true;

    protected virtual bool IncludeRowGroup(LogicalStatistics<ulong> stats) => true;

    protected virtual bool IncludeRowGroup(LogicalStatistics<sbyte> stats) => true;

    protected virtual bool IncludeRowGroup(LogicalStatistics<short> stats) => true;

    protected virtual bool IncludeRowGroup(LogicalStatistics<int> stats) => true;

    protected virtual bool IncludeRowGroup(LogicalStatistics<long> stats) => true;

    protected virtual bool IncludeRowGroup(LogicalStatistics<Half> stats) => true;

    protected virtual bool IncludeRowGroup(LogicalStatistics<float> stats) => true;

    protected virtual bool IncludeRowGroup(LogicalStatistics<double> stats) => true;
}
