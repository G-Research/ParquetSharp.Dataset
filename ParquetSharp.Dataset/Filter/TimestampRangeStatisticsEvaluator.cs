using System;

namespace ParquetSharp.Dataset.Filter;

internal sealed class TimestampRangeStatisticsEvaluator
    : ILogicalStatisticsVisitor<DateTime, bool>
        , ILogicalStatisticsVisitor<DateTimeNanos, bool>
{
    public TimestampRangeStatisticsEvaluator(DateTime start, DateTime end)
    {
        _start = start;
        _end = end;
    }

    public bool Visit(LogicalStatistics stats)
    {
        return true;
    }

    public bool Visit(LogicalStatistics<DateTime> stats)
    {
        return _end > stats.Min && _start <= stats.Max;
    }

    public bool Visit(LogicalStatistics<DateTimeNanos> stats)
    {
        return new DateTimeNanos(_end).Ticks > stats.Min.Ticks &&
               new DateTimeNanos(_start).Ticks <= stats.Max.Ticks;
    }

    private readonly DateTime _start;
    private readonly DateTime _end;
}
