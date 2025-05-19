using System;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Evaluates statistics for a row group to determine if any rows in the
/// row group might satisfy a Date typed binary comparison condition.
/// </summary>
internal sealed class DateComparisonStatisticsEvaluator
    : ILogicalStatisticsVisitor<DateOnly, bool>
{
    public DateComparisonStatisticsEvaluator(ComparisonOperator op, DateOnly value)
    {
        _operator = op;
        _value = value;
    }

    public bool Visit(LogicalStatistics stats)
    {
        return true;
    }

    public bool Visit(LogicalStatistics<DateOnly> stats)
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

    private readonly ComparisonOperator _operator;
    private readonly DateOnly _value;
}
