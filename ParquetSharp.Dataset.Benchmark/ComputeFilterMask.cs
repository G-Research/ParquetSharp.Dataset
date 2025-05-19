using System;
using System.Linq;
using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using ParquetSharp.Dataset.Filter;

namespace ParquetSharp.Dataset.Benchmark;

public class ComputeFilterMask
{
    [Params(100, 1_000, 10_000)]
    public int NumRows { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var random = new Random(0);

        var strings = new[] { "a", "bc", "def", "ghij", "klmno", "pqrstu", "vwxyz" };

        _stringArray = new StringArray.Builder()
            .AppendRange(Enumerable.Range(0, NumRows).Select(_ => strings[random.NextInt64(strings.Length)]))
            .Build();
        _intArray = new Int64Array.Builder()
            .AppendRange(Enumerable.Range(0, NumRows).Select(_ => random.NextInt64(100)))
            .Build();
        _dateArray = new Date32Array.Builder()
            .AppendRange(Enumerable.Range(0, NumRows).Select(_ => new DateOnly(2024, 1, 1).AddDays((int)random.NextInt64(100))))
            .Build();

        var stringBuilder = new StringArray.Builder();
        var intBuilder = new Int64Array.Builder();
        var dateBuilder = new Date32Array.Builder();

        for (var i = 0; i < NumRows; ++i)
        {
            if (random.NextDouble() < 0.1)
            {
                stringBuilder.AppendNull();
                intBuilder.AppendNull();
                dateBuilder.AppendNull();
            }
            else
            {
                stringBuilder.Append(strings[random.NextInt64(strings.Length)]);
                intBuilder.Append(random.NextInt64(100));
                dateBuilder.Append(new DateOnly(2024, 1, 1).AddDays((int)random.NextInt64(100)));
            }
        }

        _nullableStringArray = stringBuilder.Build();
        _nullableIntArray = intBuilder.Build();
        _nullableDateArray = dateBuilder.Build();
    }

    [Benchmark]
    public byte[] ComputeIntEqualityMask()
    {
        var evaluator = new IntComparisonEvaluator(ComparisonOperator.Equal, 10, "x");
        _intArray!.Accept(evaluator);
        return evaluator.FilterResult;
    }

    [Benchmark]
    public byte[] ComputeIntEqualityMaskWithNullableArray()
    {
        var evaluator = new IntComparisonEvaluator(ComparisonOperator.Equal, 10, "x");
        _nullableIntArray!.Accept(evaluator);
        return evaluator.FilterResult;
    }

    [Benchmark]
    public byte[] ComputeIntRangeMask()
    {
        var evaluator = new IntRangeEvaluator(10, 20, "x");
        _intArray!.Accept(evaluator);
        return evaluator.FilterResult;
    }

    [Benchmark]
    public byte[] ComputeIntRangeMaskWithNullableArray()
    {
        var evaluator = new IntRangeEvaluator(10, 20, "x");
        _nullableIntArray!.Accept(evaluator);
        return evaluator.FilterResult;
    }

    [Benchmark]
    public byte[] ComputeDateRangeMask()
    {
        var evaluator = new DateRangeEvaluator(new DateOnly(2024, 2, 1), new DateOnly(2024, 2, 29), "x");
        _dateArray!.Accept(evaluator);
        return evaluator.FilterResult;
    }

    [Benchmark]
    public byte[] ComputeDateRangeMaskWithNullableArray()
    {
        var evaluator = new DateRangeEvaluator(new DateOnly(2024, 2, 1), new DateOnly(2024, 2, 29), "x");
        _nullableDateArray!.Accept(evaluator);
        return evaluator.FilterResult;
    }

    [Benchmark]
    public byte[] ComputeStringMask()
    {
        var evaluator = new StringInSetEvaluator(new[] { "bc", "def" }, "x");
        _stringArray!.Accept(evaluator);
        return evaluator.FilterResult;
    }

    [Benchmark]
    public byte[] ComputeStringMaskWithNullableArray()
    {
        var evaluator = new StringInSetEvaluator(new[] { "bc", "def" }, "x");
        _nullableStringArray!.Accept(evaluator);
        return evaluator.FilterResult;
    }

    private IArrowArray? _stringArray;
    private IArrowArray? _nullableStringArray;
    private IArrowArray? _intArray;
    private IArrowArray? _nullableIntArray;
    private IArrowArray? _dateArray;
    private IArrowArray? _nullableDateArray;
}
