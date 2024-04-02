using Apache.Arrow;
using BenchmarkDotNet.Attributes;

namespace ParquetSharp.Dataset.Benchmark;

public class CreateConstantArray
{
    [Params(100, 1_000, 10_000)]
    public int NumRows { get; set; }

    public CreateConstantArray()
    {
        _intScalarArray = new Int32Array.Builder().Append(123).Build();
        _stringScalarArray = new StringArray.Builder().Append("abcdefg").Build();
    }

    [Benchmark]
    public IArrowArray CreateConstantIntArray()
    {
        var creator = new ConstantArrayCreator(NumRows);
        _intScalarArray.Accept(creator);
        return creator.Array!;
    }

    [Benchmark]
    public IArrowArray CreateConstantStringArray()
    {
        var creator = new ConstantArrayCreator(NumRows);
        _stringScalarArray.Accept(creator);
        return creator.Array!;
    }

    private readonly IArrowArray _intScalarArray;
    private readonly IArrowArray _stringScalarArray;
}
