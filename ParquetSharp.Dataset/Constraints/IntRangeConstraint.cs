using Apache.Arrow;

namespace ParquetSharp.Dataset.Constraints;

/// <summary>
/// Tests whether an array contains a single integer within a specified range
/// </summary>
internal sealed class IntRangeConstraint
    : IConstraintEvaluator
    , IArrowArrayVisitor<UInt8Array>
    , IArrowArrayVisitor<UInt16Array>
    , IArrowArrayVisitor<UInt32Array>
    , IArrowArrayVisitor<UInt64Array>
    , IArrowArrayVisitor<Int8Array>
    , IArrowArrayVisitor<Int16Array>
    , IArrowArrayVisitor<Int32Array>
    , IArrowArrayVisitor<Int64Array>
{
    public IntRangeConstraint(long start, long end)
    {
        _start = start;
        _end = end;
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
        throw new NotImplementedException(
            $"Integer range constraint does not support arrays with type {array.Data.DataType.Name}");
    }

    public bool Satisfied { get; private set; }

    private readonly long _start;
    private readonly long _end;
}
