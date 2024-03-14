using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Tests whether an array contains a single string within a set of specified values
/// </summary>
internal sealed class StringInSetEvaluator
    : IFilterEvaluator
    , IArrowArrayVisitor<StringArray>
{
    public StringInSetEvaluator(IReadOnlyCollection<string> values)
    {
        _allowedValues = new HashSet<string>(values);
    }

    public void Visit(StringArray array)
    {
        Satisfied = _allowedValues.Contains(array.GetString(0));
    }

    public void Visit(IArrowArray array)
    {
        throw new NotImplementedException(
            $"String filter does not support arrays with type {array.Data.DataType.Name}");
    }

    public bool Satisfied { get; private set; }

    private readonly HashSet<string> _allowedValues;
}
