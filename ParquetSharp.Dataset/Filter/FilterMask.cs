using System;
using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Holds a bitmask indicating which rows in a RecordBatch to include after filtering
/// </summary>
public sealed class FilterMask
{
    public FilterMask(ReadOnlyMemory<byte> mask)
    {
        Mask = mask;
        IncludedCount = BitUtility.CountBits(Mask.Span, 0);
    }

    public ReadOnlyMemory<byte> Mask { get; }

    public int IncludedCount { get; }
}
