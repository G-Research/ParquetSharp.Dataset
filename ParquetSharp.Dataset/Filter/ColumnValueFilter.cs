using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

internal sealed class ColumnValueFilter : IFilter
{
    internal ColumnValueFilter(string columnName, BaseFilterEvaluator evaluator)
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
            return BitUtility.GetBit(_evaluator.FilterResult, 0);
        }

        // Column not in the partition data, assume the constraint may be satisfied
        // if we're evaluating a partial dataset path, or the filter applies
        // to a column in data files.
        return true;
    }

    public FilterMask? ComputeMask(RecordBatch dataBatch)
    {
        if (dataBatch.Schema.FieldsLookup.Contains(_columnName))
        {
            var array = dataBatch.Column(_columnName);
            array.Accept(_evaluator);
            return new FilterMask(_evaluator.FilterResult);
        }

        return null;
    }

    public IEnumerable<string> Columns()
    {
        return new[] { _columnName };
    }

    private readonly string _columnName;
    private readonly BaseFilterEvaluator _evaluator;
}
