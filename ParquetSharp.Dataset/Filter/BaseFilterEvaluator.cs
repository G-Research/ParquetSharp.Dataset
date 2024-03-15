using System;
using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Computes a bitmask indicating whether a condition is satisfied per row of an array
/// </summary>
internal abstract class BaseFilterEvaluator : IArrowArrayVisitor
{
    public abstract void Visit(IArrowArray array);

    public byte[] FilterResult
    {
        get
        {
            if (_mask == null)
            {
                throw new InvalidOperationException("Array to compute filter on has not been visited");
            }

            return _mask;
        }
    }

    protected void BuildMask<TArray>(TArray array, Action<byte[], TArray> action)
        where TArray : IArrowArray
    {
        _mask = new byte[BitUtility.ByteCount(array.Length)];
        action(_mask, array);
    }

    private byte[]? _mask = null;
}
