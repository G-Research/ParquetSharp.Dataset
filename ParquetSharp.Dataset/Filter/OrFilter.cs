using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

internal sealed class OrFilter : IFilter
{
    public OrFilter(IFilter first, IFilter second)
    {
        _first = first;
        _second = second;
    }

    public bool IncludePartition(PartitionInformation partitionInformation)
    {
        return _first.IncludePartition(partitionInformation) || _second.IncludePartition(partitionInformation);
    }

    public IEnumerable<string> Columns()
    {
        return _first.Columns().Concat(_second.Columns());
    }

    public FilterMask? ComputeMask(RecordBatch dataBatch)
    {
        var firstMask = _first.ComputeMask(dataBatch);
        if (firstMask == null)
        {
            return null;
        }

        var secondMask = _second.ComputeMask(dataBatch);
        if (secondMask == null)
        {
            return null;
        }

        var numBytes = BitUtility.ByteCount(dataBatch.Length);
        var combined = new byte[numBytes];
        var firstSpan = firstMask.Mask.Span;
        var secondSpan = secondMask.Mask.Span;
        for (var byteIdx = 0; byteIdx < numBytes; ++byteIdx)
        {
            combined[byteIdx] = (byte)(firstSpan[byteIdx] | secondSpan[byteIdx]);
        }

        return new FilterMask(combined);
    }

    private readonly IFilter _first;
    private readonly IFilter _second;
}
