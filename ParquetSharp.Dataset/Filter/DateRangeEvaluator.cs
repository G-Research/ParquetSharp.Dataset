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
            for (var i = 0; i < inputArray.Length; ++i)
            {
                var value = array.GetDateOnly(i);
                var isInRange = value.HasValue && value.Value >= _start && value.Value <= _end;
                BitUtility.SetBit(mask, i, isInRange);
            }
        });
    }

    public void Visit(Date64Array array)
    {
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

    private readonly DateOnly _start;
    private readonly DateOnly _end;
    private readonly string _columnName;
}
