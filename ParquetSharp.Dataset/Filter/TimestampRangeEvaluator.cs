using System;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Tests whether an array contains a timestamp within a specified range
/// </summary>
internal sealed class TimestampRangeEvaluator :
    BaseFilterEvaluator
    , IArrowArrayVisitor<TimestampArray>
{
    public TimestampRangeEvaluator(DateTime start, DateTime end, string columnName)
    {
        _start = start;
        _end = end;
        _columnName = columnName;
    }

    public void Visit(TimestampArray array)
    {
        var dataType = array.Data.DataType;
        var timestampType = dataType as TimestampType;
        if (timestampType == null)
        {
            throw new Exception(
                $"Expected a TimestampArray to have a TimestampType DataType, got {dataType?.GetType().FullName}");
        }

        BuildMask(array, (mask, inputArray) =>
        {
            var startValue = ToPrimitiveValue(_start, timestampType.Unit);
            var endValue = ToPrimitiveValue(_end, timestampType.Unit);
            if (inputArray.NullCount == 0)
            {
                var values = inputArray.Values;
                for (var i = 0; i < inputArray.Length; ++i)
                {
                    var value = values[i];
                    var isInRange = value >= startValue && value < endValue;
                    BitUtility.SetBit(mask, i, isInRange);
                }
            }
            else
            {
                for (var i = 0; i < inputArray.Length; ++i)
                {
                    var value = inputArray.GetValue(i);
                    var isInRange = value.HasValue && value.Value >= startValue && value.Value < endValue;
                    BitUtility.SetBit(mask, i, isInRange);
                }
            }
        });
    }

    public override void Visit(IArrowArray array)
    {
        throw new NotSupportedException(
            $"Timestamp range filter for column '{_columnName}' does not support arrays with type {array.Data.DataType.Name}");
    }

    private static long ToPrimitiveValue(DateTime dateTime, Apache.Arrow.Types.TimeUnit unit)
    {
        var ticks = (dateTime - ArrowEpoch).Ticks;

        return unit switch
        {
            Apache.Arrow.Types.TimeUnit.Second => ticks / TimeSpan.TicksPerSecond,
            Apache.Arrow.Types.TimeUnit.Millisecond => ticks / TimeSpan.TicksPerMillisecond,
            Apache.Arrow.Types.TimeUnit.Microsecond => ticks / (TimeSpan.TicksPerMillisecond / 1000),
            Apache.Arrow.Types.TimeUnit.Nanosecond => checked(ticks * 100),
            _ => throw new ArgumentOutOfRangeException(nameof(unit), unit, "Invalid timestamp unit")
        };
    }

    private static readonly DateTime ArrowEpoch = new DateTime(1970, 1, 1, 0, 0, 0, 0);

    private readonly DateTime _start;
    private readonly DateTime _end;
    private readonly string _columnName;
}
