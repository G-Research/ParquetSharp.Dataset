using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Tests whether an array contains a single integer equal to a specified value
/// </summary>
internal sealed class IntEqualityEvaluator
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
    public IntEqualityEvaluator(long value, string columnName)
    {
        _expectedValue = value;
        _columnName = columnName;
    }

    public void Visit(UInt8Array array)
    {
        var value = array.GetValue(0);
        Satisfied = _expectedValue > 0 && value == _expectedValue;
    }

    public void Visit(UInt16Array array)
    {
        var value = array.GetValue(0);
        Satisfied = _expectedValue > 0 && value == _expectedValue;
    }

    public void Visit(UInt32Array array)
    {
        var value = array.GetValue(0);
        Satisfied = _expectedValue > 0 && value == (ulong) _expectedValue;
    }

    public void Visit(UInt64Array array)
    {
        var value = array.GetValue(0);
        Satisfied = _expectedValue > 0 && value == (ulong) _expectedValue;
    }

    public void Visit(Int8Array array)
    {
        Satisfied = array.GetValue(0) == _expectedValue;
    }

    public void Visit(Int16Array array)
    {
        Satisfied = array.GetValue(0) == _expectedValue;
    }

    public void Visit(Int32Array array)
    {
        Satisfied = array.GetValue(0) == _expectedValue;
    }

    public void Visit(Int64Array array)
    {
        Satisfied = array.GetValue(0) == _expectedValue;
    }

    public void Visit(IArrowArray array)
    {
        throw new NotSupportedException(
            $"Integer equality filter for column '{_columnName}' does not support arrays with type {array.Data.DataType.Name}");
    }

    public bool Satisfied { get; private set; }

    private readonly long _expectedValue;
    private readonly string _columnName;
}
