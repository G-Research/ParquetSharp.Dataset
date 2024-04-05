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
        var value = array.GetString(0);
        if (value == null)
        {
            var valueOffsetsBuilder = new ArrowBuffer.Builder<int>(_arrayLength + 1);
            valueOffsetsBuilder.Resize(_arrayLength + 1);
            valueOffsetsBuilder.Span.Fill(0);
            var valueOffsetsBuffer = valueOffsetsBuilder.Build();

            var validityBuilder = new ArrowBuffer.BitmapBuilder(_arrayLength);
            validityBuilder.AppendRange(false, _arrayLength);
            var validityBuffer = validityBuilder.Build();

            Array = new StringArray(
                _arrayLength, valueOffsetsBuffer, ArrowBuffer.Empty, validityBuffer, nullCount: _arrayLength, offset: 0);
        }
        else
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(value);
            var size = bytes.Length;

            var valueOffsetsBuilder = new ArrowBuffer.Builder<int>(_arrayLength + 1);
            valueOffsetsBuilder.Resize(_arrayLength + 1);
            var valueOffsetsSpan = valueOffsetsBuilder.Span;
            for (var i = 0; i < _arrayLength + 1; ++i)
            {
                valueOffsetsSpan[i] = i * size;
            }

            var valueOffsetsBuffer = valueOffsetsBuilder.Build();

            var dataBuilder = new ArrowBuffer.Builder<byte>(_arrayLength * size);
            dataBuilder.Resize(_arrayLength * size);
            var dataSpan = dataBuilder.Span;
            for (var i = 0; i < _arrayLength; ++i)
            {
                bytes.CopyTo(dataSpan.Slice(i * size));
            }

            var dataBuffer = dataBuilder.Build();

            Array = new StringArray(
                _arrayLength, valueOffsetsBuffer, dataBuffer, ArrowBuffer.Empty, nullCount: 0, offset: 0);
        }
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

            return new ArrayData(
                array.Data.DataType, length: _arrayLength, nullCount: 0, offset: 0,
                new[] { ArrowBuffer.Empty, valueBuffer });
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

            return new ArrayData(
                array.Data.DataType, length: _arrayLength, nullCount: _arrayLength, offset: 0,
                new[] { validityBuffer, valueBuffer });
        }
    }


    private readonly int _arrayLength;
}
