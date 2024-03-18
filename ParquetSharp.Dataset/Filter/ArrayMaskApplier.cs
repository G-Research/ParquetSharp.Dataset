using System;
using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Applies a filter mask to an array and creates a new mask with
/// only the filtered rows included.
/// </summary>
public class ArrayMaskApplier :
    IArrowArrayVisitor<UInt8Array>
    , IArrowArrayVisitor<UInt16Array>
    , IArrowArrayVisitor<UInt32Array>
    , IArrowArrayVisitor<UInt64Array>
    , IArrowArrayVisitor<Int8Array>
    , IArrowArrayVisitor<Int16Array>
    , IArrowArrayVisitor<Int32Array>
    , IArrowArrayVisitor<Int64Array>
    , IArrowArrayVisitor<HalfFloatArray>
    , IArrowArrayVisitor<FloatArray>
    , IArrowArrayVisitor<DoubleArray>
{
    public ArrayMaskApplier(FilterMask mask)
    {
        if (mask == null)
        {
            throw new ArgumentNullException(nameof(mask));
        }

        _mask = mask.Mask;
        _includedCount = mask.IncludedCount;
    }

    public IArrowArray MaskedArray
    {
        get
        {
            if (_maskedArray == null)
            {
                throw new InvalidOperationException("Array to mask has not been visited");
            }

            return _maskedArray;
        }
    }

    public void Visit(UInt8Array array)
    {
        VisitPrimitiveArray<byte, UInt8Array, UInt8Array.Builder>(array);
    }

    public void Visit(UInt16Array array)
    {
        VisitPrimitiveArray<ushort, UInt16Array, UInt16Array.Builder>(array);
    }

    public void Visit(UInt32Array array)
    {
        VisitPrimitiveArray<uint, UInt32Array, UInt32Array.Builder>(array);
    }

    public void Visit(UInt64Array array)
    {
        VisitPrimitiveArray<ulong, UInt64Array, UInt64Array.Builder>(array);
    }

    public void Visit(Int8Array array)
    {
        VisitPrimitiveArray<sbyte, Int8Array, Int8Array.Builder>(array);
    }

    public void Visit(Int16Array array)
    {
        VisitPrimitiveArray<short, Int16Array, Int16Array.Builder>(array);
    }

    public void Visit(Int32Array array)
    {
        VisitPrimitiveArray<int, Int32Array, Int32Array.Builder>(array);
    }

    public void Visit(Int64Array array)
    {
        VisitPrimitiveArray<long, Int64Array, Int64Array.Builder>(array);
    }

    public void Visit(HalfFloatArray array)
    {
        VisitPrimitiveArray<Half, HalfFloatArray, HalfFloatArray.Builder>(array);
    }

    public void Visit(FloatArray array)
    {
        VisitPrimitiveArray<float, FloatArray, FloatArray.Builder>(array);
    }

    public void Visit(DoubleArray array)
    {
        VisitPrimitiveArray<double, DoubleArray, DoubleArray.Builder>(array);
    }

    public void Visit(IArrowArray array)
    {
        throw new NotImplementedException($"Filtering an array of type {array.Data.DataType} is not implemented");
    }

    private void VisitPrimitiveArray<T, TArray, TBuilder>(TArray array)
        where T : struct
        where TArray : PrimitiveArray<T>
        where TBuilder : PrimitiveArrayBuilder<T, TArray, TBuilder>, new()
    {
        var builder = new TBuilder();
        builder.Reserve(_includedCount);

        for (var i = 0; i < array.Length; ++i)
        {
            if (BitUtility.GetBit(_mask.Span, i))
            {
                builder.Append(array.GetValue(i));
            }
        }

        _maskedArray = builder.Build();
    }

    private readonly ReadOnlyMemory<byte> _mask;
    private readonly int _includedCount;
    private IArrowArray? _maskedArray = null;
}
