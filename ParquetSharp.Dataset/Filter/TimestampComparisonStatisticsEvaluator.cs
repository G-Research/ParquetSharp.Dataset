using System;

namespace ParquetSharp.Dataset.Filter;

internal sealed class TimestampComparisonStatisticsEvaluator
    : ILogicalStatisticsVisitor<DateTime, bool>
        , ILogicalStatisticsVisitor<DateTimeNanos, bool>
{
    public TimestampComparisonStatisticsEvaluator(ComparisonOperator op, DateTime value)
    {
        _operator = op;
        _value = value;
    }

    public bool Visit(LogicalStatistics stats)
    {
        return true;
    }

    public bool Visit(LogicalStatistics<DateTime> stats)
    {
        switch (_operator)
        {
            case ComparisonOperator.Equal:
                return _value >= stats.Min && _value <= stats.Max;
            case ComparisonOperator.GreaterThan:
                return stats.Max > _value;
            case ComparisonOperator.GreaterThanOrEqual:
                return stats.Max >= _value;
            case ComparisonOperator.LessThan:
                return stats.Min < _value;
            case ComparisonOperator.LessThanOrEqual:
                return stats.Min <= _value;
            default:
                throw new Exception($"Unexpected comparison operator {_operator}");
        }
    }

    public bool Visit(LogicalStatistics<DateTimeNanos> stats)
    {
        var valueNanos = new DateTimeNanos(_value).Ticks;
        switch (_operator)
        {
            case ComparisonOperator.Equal:
                return valueNanos >= stats.Min.Ticks && valueNanos <= stats.Max.Ticks;
            case ComparisonOperator.GreaterThan:
                return stats.Max.Ticks > valueNanos;
            case ComparisonOperator.GreaterThanOrEqual:
                return stats.Max.Ticks >= valueNanos;
            case ComparisonOperator.LessThan:
                return stats.Min.Ticks < valueNanos;
            case ComparisonOperator.LessThanOrEqual:
                return stats.Min.Ticks <= valueNanos;
            default:
                throw new Exception($"Unexpected comparison operator {_operator}");
        }
    }

    private readonly ComparisonOperator _operator;
    private readonly DateTime _value;
}
