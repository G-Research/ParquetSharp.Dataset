using System;
using System.Linq;
using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using ParquetSharp.Dataset.Filter;

namespace ParquetSharp.Dataset.Benchmark;

public class ApplyFilterMask
{
    [Params(100, 1_000, 10_000)]
    public int NumRows { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var random = new Random(0);

        _intArray = new Int64Array.Builder()
            .AppendRange(Enumerable.Range(0, NumRows).Select(_ => random.NextInt64(100)))
            .Build();

        var intBuilder = new Int64Array.Builder();
        var mask = new byte[BitUtility.ByteCount(NumRows)];

        for (var i = 0; i < NumRows; ++i)
        {
            BitUtility.SetBit(mask, i, random.NextDouble() < 0.5);
            if (random.NextDouble() < 0.1)
            {
                intBuilder.AppendNull();
            }
            else
            {
                intBuilder.Append(random.NextInt64(100));
            }
        }

        _nullableIntArray = intBuilder.Build();
        _maskApplier = new ArrayMaskApplier(new FilterMask(mask));
    }

    [Benchmark]
    public IArrowArray FilterInt64Array()
    {
        _intArray!.Accept(_maskApplier);
        return _maskApplier!.MaskedArray;
    }

    [Benchmark]
    public IArrowArray FilterNullableInt64Array()
    {
        _nullableIntArray!.Accept(_maskApplier);
        return _maskApplier!.MaskedArray;
    }

    private IArrowArray? _intArray;
    private IArrowArray? _nullableIntArray;
    private ArrayMaskApplier? _maskApplier;
}
