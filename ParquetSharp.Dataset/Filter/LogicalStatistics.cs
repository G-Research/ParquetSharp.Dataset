using System;

namespace ParquetSharp.Dataset.Filter;

public abstract class LogicalStatistics
{
    public static LogicalStatistics? FromStatistics(Statistics? statistics, ColumnDescriptor descriptor)
    {
        if (!(statistics?.HasMinMax ?? false))
        {
            return null;
        }

        using var logicalType = descriptor.LogicalType;
        checked
        {
            return (statistics, logicalType) switch
            {
                (Statistics<bool> stats, NoneLogicalType) => new LogicalStatistics<bool>(stats.Min, stats.Max),
                (Statistics<int> stats, NoneLogicalType) => new LogicalStatistics<int>(stats.Min, stats.Max),
                (Statistics<long> stats, NoneLogicalType) => new LogicalStatistics<long>(stats.Min, stats.Max),
                (Statistics<int> stats, IntLogicalType { BitWidth: 8, IsSigned: true }) => new LogicalStatistics<sbyte>(
                    (sbyte)stats.Min, (sbyte)stats.Max),
                (Statistics<int> stats, IntLogicalType { BitWidth: 8, IsSigned: false }) => new LogicalStatistics<byte>(
                    (byte)unchecked((uint)stats.Min), (byte)unchecked((uint)stats.Max)),
                (Statistics<int> stats, IntLogicalType { BitWidth: 16, IsSigned: true }) => new LogicalStatistics<short>(
                    (short)stats.Min, (short)stats.Max),
                (Statistics<int> stats, IntLogicalType { BitWidth: 16, IsSigned: false }) => new LogicalStatistics<ushort>(
                    (ushort)unchecked((uint)stats.Min), (ushort)unchecked((uint)stats.Max)),
                (Statistics<int> stats, IntLogicalType { BitWidth: 32, IsSigned: true }) => new LogicalStatistics<int>(
                    stats.Min, stats.Max),
                (Statistics<int> stats, IntLogicalType { BitWidth: 32, IsSigned: false }) => new LogicalStatistics<uint>(
                    unchecked((uint)stats.Min), unchecked((uint)stats.Max)),
                (Statistics<long> stats, IntLogicalType { BitWidth: 64, IsSigned: true }) => new LogicalStatistics<long>(
                    stats.Min, stats.Max),
                (Statistics<long> stats, IntLogicalType { BitWidth: 64, IsSigned: false }) => new LogicalStatistics<ulong>(
                    unchecked((ulong)stats.Min), unchecked((ulong)stats.Max)),
                (Statistics<FixedLenByteArray> stats, Float16LogicalType) => new LogicalStatistics<Half>(
                    LogicalRead.ToHalf(stats.Min), LogicalRead.ToHalf(stats.Max)),
                (Statistics<float> stats, NoneLogicalType) => new LogicalStatistics<float>(stats.Min, stats.Max),
                (Statistics<double> stats, NoneLogicalType) => new LogicalStatistics<double>(stats.Min, stats.Max),
                _ => null,
            };
        }
    }

    public abstract TOut Accept<TOut>(ILogicalStatisticsVisitor<TOut> visitor);
}

/// <summary>
/// Parquet column statistics converted to logical typed values
/// </summary>
public sealed class LogicalStatistics<T> : LogicalStatistics
{
    internal LogicalStatistics(T min, T max)
    {
        Min = min;
        Max = max;
    }

    public T Min { get; }

    public T Max { get; }

    public override TOut Accept<TOut>(ILogicalStatisticsVisitor<TOut> visitor)
    {
        if (visitor is ILogicalStatisticsVisitor<T, TOut> typedVisitor)
        {
            return typedVisitor.Visit(this);
        }
        else
        {
            return visitor.Visit(this);
        }
    }
}
