using System;

namespace ParquetSharp.Dataset.Filter;

internal sealed class DateRangeStatisticsEvaluator
    : ILogicalStatisticsVisitor<DateOnly, bool>
{
    public DateRangeStatisticsEvaluator(DateOnly start, DateOnly end)
    {
        _start = start;
        _end = end;
    }

    public bool Visit(LogicalStatistics stats)
    {
        return true;
    }

    public bool Visit(LogicalStatistics<DateOnly> stats)
    {
        return _end > stats.Min && _start <= stats.Max;
    }

    private readonly DateOnly _start;
    private readonly DateOnly _end;
}
