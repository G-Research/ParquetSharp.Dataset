using Apache.Arrow;

namespace ParquetSharp.Dataset.Constraints;

/// <summary>
/// Tests whether an array contains a single string equal to a specified value
/// </summary>
internal sealed class StringInSetConstraint
    : IConstraintEvaluator
    , IArrowArrayVisitor<StringArray>
{
    public StringInSetConstraint(IReadOnlyCollection<string> values)
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
            $"String constraint does not support arrays with type {array.Data.DataType.Name}");
    }

    public bool Satisfied { get; private set; }

    private readonly HashSet<string> _allowedValues;
}
