using System;
using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Tests whether an array contains a date within a specified date range
/// </summary>
internal sealed class DateRangeEvaluator :
    BaseFilterEvaluator
    , IArrowArrayVisitor<Date32Array>
    , IArrowArrayVisitor<Date64Array>
{
    public DateRangeEvaluator(DateOnly start, DateOnly end, string columnName)
    {
        _start = start;
        _end = end;
        _columnName = columnName;
    }

    public void Visit(Date32Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            var startNumber = _start.DayNumber - ArrowEpoch.DayNumber;
            var endNumber = _end.DayNumber - ArrowEpoch.DayNumber;
            if (inputArray.NullCount == 0)
            {
                var values = array.Values;
                for (var i = 0; i < inputArray.Length; ++i)
                {
                    var value = values[i];
                    var isInRange = value >= startNumber && value <= endNumber;
                    BitUtility.SetBit(mask, i, isInRange);
                }
            }
            else
            {
                for (var i = 0; i < inputArray.Length; ++i)
                {
                    var value = array.GetValue(i);
                    var isInRange = value.HasValue && value.Value >= startNumber && value.Value <= endNumber;
                    BitUtility.SetBit(mask, i, isInRange);
                }
            }
        });
    }

    public void Visit(Date64Array array)
    {
        // A Date64Array holds the number of milliseconds since the UNIX epoch,
        // so the conversion from a long value to a date isn't as simple as for Date32
        BuildMask(array, (mask, inputArray) =>
        {
            for (var i = 0; i < inputArray.Length; ++i)
            {
                var value = array.GetDateOnly(i);
                var isInRange = value.HasValue && value.Value >= _start && value.Value <= _end;
                BitUtility.SetBit(mask, i, isInRange);
            }
        });
    }

    public override void Visit(IArrowArray array)
    {
        throw new NotSupportedException(
            $"Date range filter for column '{_columnName}' does not support arrays with type {array.Data.DataType.Name}");
    }

    private static readonly DateOnly ArrowEpoch = new DateOnly(1970, 1, 1);

    private readonly DateOnly _start;
    private readonly DateOnly _end;
    private readonly string _columnName;
}
