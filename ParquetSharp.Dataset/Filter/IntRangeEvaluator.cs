using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Tests whether an array contains a single integer within a specified range
/// </summary>
internal sealed class IntRangeEvaluator
    : IFilterEvaluator
    , IArrowArrayVisitor<UInt8Array>
    , IArrowArrayVisitor<UInt16Array>
    , IArrowArrayVisitor<UInt32Array>
    , IArrowArrayVisitor<UInt64Array>
    , IArrowArrayVisitor<Int8Array>
    , IArrowArrayVisitor<Int16Array>
    , IArrowArrayVisitor<Int32Array>
    , IArrowArrayVisitor<Int64Array>
{
    public IntRangeEvaluator(long start, long end, string columnName)
    {
        _start = start;
        _end = end;
        _columnName = columnName;
    }

    public void Visit(UInt8Array array)
    {
        var value = array.GetValue(0);
        var geStart = _start < 0 || value >= (ulong) _start;
        var leEnd = _end >= 0 && value <= (ulong) _end;
        Satisfied = geStart && leEnd;
    }

    public void Visit(UInt16Array array)
    {
        var value = array.GetValue(0);
        var geStart = _start < 0 || value >= (ulong) _start;
        var leEnd = _end >= 0 && value <= (ulong) _end;
        Satisfied = geStart && leEnd;
    }

    public void Visit(UInt32Array array)
    {
        var value = array.GetValue(0);
        var geStart = _start < 0 || value >= (ulong) _start;
        var leEnd = _end >= 0 && value <= (ulong) _end;
        Satisfied = geStart && leEnd;
    }

    public void Visit(UInt64Array array)
    {
        var value = array.GetValue(0);
        var geStart = _start < 0 || value >= (ulong) _start;
        var leEnd = _end >= 0 && value <= (ulong) _end;
        Satisfied = geStart && leEnd;
    }

    public void Visit(Int8Array array)
    {
        var value = array.GetValue(0);
        Satisfied = value >= _start && value <= _end;
    }

    public void Visit(Int16Array array)
    {
        var value = array.GetValue(0);
        Satisfied = value >= _start && value <= _end;
    }

    public void Visit(Int32Array array)
    {
        var value = array.GetValue(0);
        Satisfied = value >= _start && value <= _end;
    }

    public void Visit(Int64Array array)
    {
        var value = array.GetValue(0);
        Satisfied = value >= _start && value <= _end;
    }

    public void Visit(IArrowArray array)
    {
        throw new NotSupportedException(
            $"Integer range filter for column '{_columnName}' does not support arrays with type {array.Data.DataType.Name}");
    }

    public bool Satisfied { get; private set; }

    private readonly long _start;
    private readonly long _end;
    private readonly string _columnName;
}
