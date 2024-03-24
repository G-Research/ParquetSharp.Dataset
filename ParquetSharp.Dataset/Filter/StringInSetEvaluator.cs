using System;
using System.Collections.Generic;
using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Tests whether an array contains a single string within a set of specified values
/// </summary>
internal sealed class StringInSetEvaluator : BaseFilterEvaluator, IArrowArrayVisitor<StringArray>
{
    public StringInSetEvaluator(IReadOnlyCollection<string?> values, string columnName)
    {
        _columnName = columnName;
        _allowedValues = new HashSet<string?>(values);
    }

    public void Visit(StringArray array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            for (var i = 0; i < inputArray.Length; ++i)
            {
                BitUtility.SetBit(mask, i, _allowedValues.Contains(array.GetString(i)));
            }
        });
    }

    public override void Visit(IArrowArray array)
    {
        throw new NotSupportedException(
            $"String filter for column '{_columnName}' does not support arrays with type {array.Data.DataType.Name}");
    }

    private readonly HashSet<string?> _allowedValues;
    private readonly string _columnName;
}
