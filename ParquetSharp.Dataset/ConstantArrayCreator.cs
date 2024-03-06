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

    private void VisitPrimitiveArray<T, TArray, TBuilder>(TArray array)
        where T: struct
        where TArray: PrimitiveArray<T>
        where TBuilder: PrimitiveArrayBuilder<T, TArray, TBuilder>, new()
    {
        var builder = new TBuilder();
        builder.Reserve(_arrayLength);
        var value = array.GetValue(0);
        if (value.HasValue)
        {
            for (var i = 0; i < _arrayLength; ++i)
            {
                builder.Append(value.Value);
            }
        }
        else
        {
            for (var i = 0; i < _arrayLength; ++i)
            {
                builder.AppendNull();
            }
        }

        Array = builder.Build();
    }


    private readonly int _arrayLength;
}
