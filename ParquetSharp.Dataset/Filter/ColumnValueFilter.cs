using System.Collections.Generic;

namespace ParquetSharp.Dataset.Filter;

internal sealed class ColumnValueFilter : IFilter
{
    internal ColumnValueFilter(string columnName, IFilterEvaluator evaluator)
    {
        _columnName = columnName;
        _evaluator = evaluator;
    }

    public bool IncludePartition(PartitionInformation partitionInfo)
    {
        if (partitionInfo.Batch.Schema.FieldsLookup.Contains(_columnName))
        {
            var scalarArray = partitionInfo.Batch.Column(_columnName);
            scalarArray.Accept(_evaluator);
            return _evaluator.Satisfied;
        }

        // Column not in the partition data, assume the constraint may be satisfied
        // if we're evaluating a partial dataset path, or the filter applies
        // to a column in data files.
        return true;
    }

    public IEnumerable<string> Columns()
    {
        return new[] { _columnName };
    }

    private readonly string _columnName;
    private readonly IFilterEvaluator _evaluator;
}
