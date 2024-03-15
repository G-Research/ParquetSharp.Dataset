using System;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace ParquetSharp.Dataset;

/// <summary>
/// Creates scalar (single valued) arrays by parsing a string value
/// </summary>
internal sealed class ScalarParser :
    IArrowTypeVisitor<StringType>,
    IArrowTypeVisitor<UInt8Type>,
    IArrowTypeVisitor<UInt16Type>,
    IArrowTypeVisitor<UInt32Type>,
    IArrowTypeVisitor<UInt64Type>,
    IArrowTypeVisitor<Int8Type>,
    IArrowTypeVisitor<Int16Type>,
    IArrowTypeVisitor<Int32Type>,
    IArrowTypeVisitor<Int64Type>
{
    public ScalarParser(string? value)
    {
        _value = value;
    }

    public IArrowArray? ScalarArray { get; private set; }

    public void Visit(StringType type)
    {
        ScalarArray = new StringArray.Builder().Append(_value).Build();
    }

    public void Visit(UInt8Type type)
    {
        var builder = new UInt8Array.Builder();
        ScalarArray = (
                _value == null
                    ? builder.AppendNull()
                    : builder.Append(byte.Parse(_value)))
            .Build();
    }

    public void Visit(UInt16Type type)
    {
        var builder = new UInt16Array.Builder();
        ScalarArray = (
                _value == null
                    ? builder.AppendNull()
                    : builder.Append(ushort.Parse(_value)))
            .Build();
    }

    public void Visit(UInt32Type type)
    {
        var builder = new UInt32Array.Builder();
        ScalarArray = (
                _value == null
                    ? builder.AppendNull()
                    : builder.Append(uint.Parse(_value)))
            .Build();
    }

    public void Visit(UInt64Type type)
    {
        var builder = new UInt64Array.Builder();
        ScalarArray = (
                _value == null
                    ? builder.AppendNull()
                    : builder.Append(ulong.Parse(_value)))
            .Build();
    }

    public void Visit(Int8Type type)
    {
        var builder = new Int8Array.Builder();
        ScalarArray = (
                _value == null
                    ? builder.AppendNull()
                    : builder.Append(sbyte.Parse(_value)))
            .Build();
    }

    public void Visit(Int16Type type)
    {
        var builder = new Int16Array.Builder();
        ScalarArray = (
                _value == null
                    ? builder.AppendNull()
                    : builder.Append(short.Parse(_value)))
            .Build();
    }

    public void Visit(Int32Type type)
    {
        var builder = new Int32Array.Builder();
        ScalarArray = (
                _value == null
                    ? builder.AppendNull()
                    : builder.Append(int.Parse(_value)))
            .Build();
    }

    public void Visit(Int64Type type)
    {
        var builder = new Int64Array.Builder();
        ScalarArray = (
                _value == null
                    ? builder.AppendNull()
                    : builder.Append(long.Parse(_value)))
            .Build();
    }

    public void Visit(IArrowType type)
    {
        throw new NotImplementedException(
            $"Cannot create scalar array for type '{type}'");
    }

    private readonly string? _value;
}
