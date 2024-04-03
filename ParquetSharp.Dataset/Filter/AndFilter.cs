using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

internal sealed class AndFilter : IFilter
{
    public AndFilter(IFilter first, IFilter second)
    {
        _first = first;
        _second = second;
    }

    public bool IncludePartition(PartitionInformation partitionInformation)
    {
        return _first.IncludePartition(partitionInformation) && _second.IncludePartition(partitionInformation);
    }

    public FilterMask? ComputeMask(RecordBatch dataBatch)
    {
        throw new NotImplementedException();
    }

    public IEnumerable<string> Columns()
    {
        return _first.Columns().Concat(_second.Columns());
    }

    private readonly IFilter _first;
    private readonly IFilter _second;
}
