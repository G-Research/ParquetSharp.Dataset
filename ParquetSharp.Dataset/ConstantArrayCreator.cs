using System;
using Apache.Arrow;

namespace ParquetSharp.Dataset;

/// <summary>
/// Creates an array that repeats a constant value
/// </summary>
internal sealed class ConstantArrayCreator
    : IArrowArrayVisitor<UInt8Array>
        , IArrowArrayVisitor<UInt16Array>
        , IArrowArrayVisitor<UInt32Array>
        , IArrowArrayVisitor<UInt64Array>
        , IArrowArrayVisitor<Int8Array>
        , IArrowArrayVisitor<Int16Array>
        , IArrowArrayVisitor<Int32Array>
        , IArrowArrayVisitor<Int64Array>
        , IArrowArrayVisitor<StringArray>
{
    public ConstantArrayCreator(int arrayLength)
    {
        _arrayLength = arrayLength;
    }

    public IArrowArray? Array { get; private set; }

    public void Visit(UInt8Array array)
    {
        Array = new UInt8Array(VisitPrimitiveArray<byte, UInt8Array>(array));
    }

    public void Visit(UInt16Array array)
    {
        Array = new UInt16Array(VisitPrimitiveArray<ushort, UInt16Array>(array));
    }

    public void Visit(UInt32Array array)
    {
        Array = new UInt32Array(VisitPrimitiveArray<uint, UInt32Array>(array));
    }

    public void Visit(UInt64Array array)
    {
        Array = new UInt64Array(VisitPrimitiveArray<ulong, UInt64Array>(array));
    }

    public void Visit(Int8Array array)
    {
        Array = new Int8Array(VisitPrimitiveArray<sbyte, Int8Array>(array));
    }

    public void Visit(Int16Array array)
    {
        Array = new Int16Array(VisitPrimitiveArray<short, Int16Array>(array));
    }

    public void Visit(Int32Array array)
    {
        Array = new Int32Array(VisitPrimitiveArray<int, Int32Array>(array));
    }

    public void Visit(Int64Array array)
    {
        Array = new Int64Array(VisitPrimitiveArray<long, Int64Array>(array));
    }

    public void Visit(StringArray array)
    {
        var builder = new StringArray.Builder();
        builder.Reserve(_arrayLength);
        var value = array.GetString(0);
        if (value == null)
        {
            for (var i = 0; i < _arrayLength; ++i)
            {
                builder.AppendNull();
            }
        }
        else
        {
            for (var i = 0; i < _arrayLength; ++i)
            {
                builder.Append(value);
            }
        }

        Array = builder.Build();
    }

    public void Visit(IArrowArray array)
    {
        throw new NotImplementedException(
            $"Cannot create a constant array with type {array.Data.DataType.Name}");
    }

    private ArrayData VisitPrimitiveArray<T, TArray>(TArray array)
        where T : struct
        where TArray : PrimitiveArray<T>
    {
        var value = array.GetValue(0);
        if (value.HasValue)
        {
            var valueBuilder = new ArrowBuffer.Builder<T>(_arrayLength);
            valueBuilder.Resize(_arrayLength);
            valueBuilder.Span.Fill(value.Value);
            var valueBuffer = valueBuilder.Build();

            return new ArrayData(array.Data.DataType, _arrayLength, 0, 0, new[] { ArrowBuffer.Empty, valueBuffer });
        }
        else
        {
            var valueBuilder = new ArrowBuffer.Builder<T>(_arrayLength);
            valueBuilder.Resize(_arrayLength);
            valueBuilder.Span.Fill(default);
            var valueBuffer = valueBuilder.Build();

            var validityBuilder = new ArrowBuffer.BitmapBuilder(_arrayLength);
            validityBuilder.AppendRange(false, _arrayLength);
            var validityBuffer = validityBuilder.Build();

            return new ArrayData(array.Data.DataType, _arrayLength, _arrayLength, 0, new[] { validityBuffer, valueBuffer });
        }
    }


    private readonly int _arrayLength;
}
