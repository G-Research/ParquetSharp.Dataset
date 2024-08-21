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
                (Statistics<bool> stats, NoneLogicalType) => CreateStatistics<bool, bool>(stats, val => val),
                (Statistics<int> stats, NoneLogicalType) => CreateStatistics<int, int>(stats, val => val),
                (Statistics<long> stats, NoneLogicalType) => CreateStatistics<long, long>(stats, val => val),
                (Statistics<int> stats, IntLogicalType { BitWidth: 8, IsSigned: true }) => CreateStatistics<int, sbyte>(stats, val => (sbyte)val),
                (Statistics<int> stats, IntLogicalType { BitWidth: 8, IsSigned: false }) => CreateStatistics<int, byte>(stats, val => (byte)unchecked((uint)val)),
                (Statistics<int> stats, IntLogicalType { BitWidth: 16, IsSigned: true }) => CreateStatistics<int, short>(stats, val => (short)val),
                (Statistics<int> stats, IntLogicalType { BitWidth: 16, IsSigned: false }) => CreateStatistics<int, ushort>(stats, val => (ushort)unchecked((uint)val)),
                (Statistics<int> stats, IntLogicalType { BitWidth: 32, IsSigned: true }) => CreateStatistics<int, int>(stats, val => val),
                (Statistics<int> stats, IntLogicalType { BitWidth: 32, IsSigned: false }) => CreateStatistics<int, uint>(stats, val => unchecked((uint)val)),
                (Statistics<long> stats, IntLogicalType { BitWidth: 64, IsSigned: true }) => CreateStatistics<long, long>(stats, val => val),
                (Statistics<long> stats, IntLogicalType { BitWidth: 64, IsSigned: false }) => CreateStatistics<long, ulong>(stats, val => unchecked((ulong)val)),
                (Statistics<FixedLenByteArray> stats, Float16LogicalType) => CreateStatistics<FixedLenByteArray, Half>(stats, LogicalRead.ToHalf),
                (Statistics<float> stats, NoneLogicalType) => CreateStatistics<float, float>(stats, val => val),
                (Statistics<double> stats, NoneLogicalType) => CreateStatistics<double, double>(stats, val => val),
                (Statistics<int> stats, DateLogicalType) => CreateStatistics<int, DateOnly>(stats, LogicalRead.ToDateOnly),
                (Statistics<long> stats, TimestampLogicalType timestampType) => timestampType.TimeUnit switch
                {
                    TimeUnit.Millis => CreateStatistics<long, DateTime>(stats, LogicalRead.ToDateTimeMillis),
                    TimeUnit.Micros => CreateStatistics<long, DateTime>(stats, LogicalRead.ToDateTimeMicros),
                    TimeUnit.Nanos => CreateStatistics<long, DateTimeNanos>(stats, value => new DateTimeNanos(value)),
                    _ => null,
                },
                _ => null,
            };
        }
    }

    private static LogicalStatistics<TStats> CreateStatistics<TPhysical, TStats>(
        Statistics<TPhysical> statistics, Func<TPhysical, TStats> converter)
        where TPhysical : unmanaged
    {
        return new LogicalStatistics<TStats>(converter(statistics.Min), converter(statistics.Max));
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
