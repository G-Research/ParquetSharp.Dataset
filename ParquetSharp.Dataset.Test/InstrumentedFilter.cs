using System.Collections.Generic;
using Apache.Arrow;
using ParquetSharp.Dataset.Filter;

namespace ParquetSharp.Dataset.Test;

internal sealed class InstrumentedFilter : IFilter
{
    public InstrumentedFilter(IFilter filter)
    {
        _filter = filter;
    }

    public int IncludePartitionCallCount { get; private set; }

    public int IncludeRowGroupCallCount { get; private set; }

    public int ComputeMaskRowCount { get; private set; }

    public bool IncludePartition(PartitionInformation partitionInformation)
    {
        ++IncludePartitionCallCount;
        return _filter.IncludePartition(partitionInformation);
    }

    public bool IncludeRowGroup(IReadOnlyDictionary<string, LogicalStatistics> columnStatistics)
    {
        ++IncludeRowGroupCallCount;
        return _filter.IncludeRowGroup(columnStatistics);
    }

    public FilterMask? ComputeMask(RecordBatch dataBatch)
    {
        ComputeMaskRowCount += dataBatch.Length;
        return _filter.ComputeMask(dataBatch);
    }

    public IEnumerable<string> Columns()
    {
        return _filter.Columns();
    }

    private readonly IFilter _filter;
}
