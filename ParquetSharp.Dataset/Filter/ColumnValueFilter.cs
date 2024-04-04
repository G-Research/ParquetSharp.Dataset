using System.Collections.Generic;
using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

internal sealed class ColumnValueFilter : IFilter
{
    internal ColumnValueFilter(
        string columnName,
        BaseFilterEvaluator evaluator,
        ILogicalStatisticsVisitor<bool>? statsEvaluator = null)
    {
        _columnName = columnName;
        _evaluator = evaluator;
        _statsEvaluator = statsEvaluator;
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

    public bool IncludeRowGroup(IReadOnlyDictionary<string, LogicalStatistics> columnStatistics)
    {
        if (_statsEvaluator != null && columnStatistics.TryGetValue(_columnName, out var statistics))
        {
            return statistics.Accept(_statsEvaluator);
        }

        // Filter column is not a Parquet column or does not have statistics
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
    private readonly ILogicalStatisticsVisitor<bool>? _statsEvaluator;
}
