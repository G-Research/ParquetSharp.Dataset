using System;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Evaluates statistics for a row group to determine if any rows in the
/// row group might satisfy a binary comparison condition.
/// </summary>
internal sealed class IntComparisonStatisticsEvaluator
    : ILogicalStatisticsVisitor<byte, bool>,
        ILogicalStatisticsVisitor<ushort, bool>,
        ILogicalStatisticsVisitor<uint, bool>,
        ILogicalStatisticsVisitor<ulong, bool>,
        ILogicalStatisticsVisitor<sbyte, bool>,
        ILogicalStatisticsVisitor<short, bool>,
        ILogicalStatisticsVisitor<int, bool>,
        ILogicalStatisticsVisitor<long, bool>
{
    public IntComparisonStatisticsEvaluator(ComparisonOperator op, long value)
    {
        _operator = op;
        _value = value;
    }

    public bool Visit(LogicalStatistics stats)
    {
        return true;
    }

    public bool Visit(LogicalStatistics<byte> stats)
    {
        return VisitLongStats(stats.Min, stats.Max);
    }

    public bool Visit(LogicalStatistics<ushort> stats)
    {
        return VisitLongStats(stats.Min, stats.Max);
    }

    public bool Visit(LogicalStatistics<uint> stats)
    {
        return VisitLongStats(stats.Min, stats.Max);
    }

    public bool Visit(LogicalStatistics<ulong> stats)
    {
        switch (_operator)
        {
            case ComparisonOperator.Equal:
            {
                return _value >= 0 && (ulong)_value >= stats.Min && (ulong)_value <= stats.Max;
            }
            case ComparisonOperator.GreaterThan:
            {
                return _value < 0 || stats.Max > (ulong)_value;
            }
            case ComparisonOperator.GreaterThanOrEqual:
            {
                return _value < 0 || stats.Max >= (ulong)_value;
            }
            case ComparisonOperator.LessThan:
            {
                return _value > 0 && stats.Min < (ulong)_value;
            }
            case ComparisonOperator.LessThanOrEqual:
            {
                return _value >= 0 && stats.Min <= (ulong)_value;
            }
            default:
                throw new Exception($"Unexpected comparison operator {_operator}");
        }
    }

    public bool Visit(LogicalStatistics<sbyte> stats)
    {
        return VisitLongStats(stats.Min, stats.Max);
    }

    public bool Visit(LogicalStatistics<short> stats)
    {
        return VisitLongStats(stats.Min, stats.Max);
    }

    public bool Visit(LogicalStatistics<int> stats)
    {
        return VisitLongStats(stats.Min, stats.Max);
    }

    public bool Visit(LogicalStatistics<long> stats)
    {
        return VisitLongStats(stats.Min, stats.Max);
    }

    private bool VisitLongStats(long minValue, long maxValue)
    {
        switch (_operator)
        {
            case ComparisonOperator.Equal:
            {
                return _value >= minValue && _value <= maxValue;
            }
            case ComparisonOperator.GreaterThan:
            {
                return maxValue > _value;
            }
            case ComparisonOperator.GreaterThanOrEqual:
            {
                return maxValue >= _value;
            }
            case ComparisonOperator.LessThan:
            {
                return minValue < _value;
            }
            case ComparisonOperator.LessThanOrEqual:
            {
                return minValue <= _value;
            }
            default:
                throw new Exception($"Unexpected comparison operator {_operator}");
        }
    }

    private readonly ComparisonOperator _operator;
    private readonly long _value;
}
