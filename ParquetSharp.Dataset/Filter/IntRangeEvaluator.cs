using System;
using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Tests whether an array contains a single integer within a specified range
/// </summary>
internal sealed class IntRangeEvaluator :
    BaseFilterEvaluator
    , IArrowArrayVisitor<UInt8Array>
    , IArrowArrayVisitor<UInt16Array>
    , IArrowArrayVisitor<UInt32Array>
    , IArrowArrayVisitor<UInt64Array>
    , IArrowArrayVisitor<Int8Array>
    , IArrowArrayVisitor<Int16Array>
    , IArrowArrayVisitor<Int32Array>
    , IArrowArrayVisitor<Int64Array>
{
    public IntRangeEvaluator(long start, long end, string columnName)
    {
        _start = start;
        _end = end;
        _columnName = columnName;
    }

    public void Visit(UInt8Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_end < 0 || _start > byte.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                ComputeMask(mask, inputArray, (byte)Math.Max(_start, 0L), (byte)Math.Min(_end, byte.MaxValue));
            }
        });
    }

    public void Visit(UInt16Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_end < 0 || _start > ushort.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                ComputeMask(mask, inputArray, (ushort)Math.Max(_start, 0L), (ushort)Math.Min(_end, ushort.MaxValue));
            }
        });
    }

    public void Visit(UInt32Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_end < 0 || _start > uint.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                ComputeMask(mask, inputArray, (uint)Math.Max(_start, 0L), (uint)Math.Min(_end, uint.MaxValue));
            }
        });
    }

    public void Visit(UInt64Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_end < 0)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                ComputeMask(mask, inputArray, (ulong)Math.Max(_start, 0L), (ulong)_end);
            }
        });
    }

    public void Visit(Int8Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_end < sbyte.MinValue || _start > sbyte.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                ComputeMask(mask, inputArray, (sbyte)Math.Max(_start, sbyte.MinValue), (sbyte)Math.Min(_end, sbyte.MaxValue));
            }
        });
    }

    public void Visit(Int16Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_end < short.MinValue || _start > short.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                ComputeMask(mask, inputArray, (short)Math.Max(_start, short.MinValue), (short)Math.Min(_end, short.MaxValue));
            }
        });
    }

    public void Visit(Int32Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_end < int.MinValue || _start > int.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                ComputeMask(mask, inputArray, (int)Math.Max(_start, int.MinValue), (int)Math.Min(_end, int.MaxValue));
            }
        });
    }

    public void Visit(Int64Array array)
    {
        BuildMask(array, (mask, inputArray) => ComputeMask(mask, inputArray, _start, _end));
    }

    private static void ComputeMask<T, TArray>(byte[] mask, TArray array, T rangeStart, T rangeEnd)
        where T : struct, IComparable<T>
        where TArray : PrimitiveArray<T>
    {
        if (array.NullCount == 0)
        {
            var values = array.Values;
            for (var i = 0; i < array.Length; ++i)
            {
                var value = values[i];
                BitUtility.SetBit(mask, i, value.CompareTo(rangeStart) >= 0 && value.CompareTo(rangeEnd) <= 0);
            }
        }
        else
        {
            for (var i = 0; i < array.Length; ++i)
            {
                var value = array.GetValue(i);
                BitUtility.SetBit(
                    mask, i,
                    value.HasValue && value.Value.CompareTo(rangeStart) >= 0 && value.Value.CompareTo(rangeEnd) <= 0);
            }
        }
    }

    public override void Visit(IArrowArray array)
    {
        throw new NotSupportedException(
            $"Integer range filter for column '{_columnName}' does not support arrays with type {array.Data.DataType.Name}");
    }

    private readonly long _start;
    private readonly long _end;
    private readonly string _columnName;
}
