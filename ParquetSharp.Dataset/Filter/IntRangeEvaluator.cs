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
            if (_end <= 0 || _start > byte.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                var end = _end > byte.MaxValue ? null : (byte?)_end;
                ComputeMask(mask, inputArray, (byte)Math.Max(_start, 0L), end);
            }
        });
    }

    public void Visit(UInt16Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_end <= 0 || _start > ushort.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                var end = _end > ushort.MaxValue ? null : (ushort?)_end;
                ComputeMask(mask, inputArray, (ushort)Math.Max(_start, 0L), end);
            }
        });
    }

    public void Visit(UInt32Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_end <= 0 || _start > uint.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                var end = _end > uint.MaxValue ? null : (uint?)_end;
                ComputeMask(mask, inputArray, (uint)Math.Max(_start, 0L), end);
            }
        });
    }

    public void Visit(UInt64Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_end <= 0)
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
            if (_end <= sbyte.MinValue || _start > sbyte.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                var end = _end > sbyte.MaxValue ? null : (sbyte?)_end;
                ComputeMask(mask, inputArray, (sbyte)Math.Max(_start, sbyte.MinValue), end);
            }
        });
    }

    public void Visit(Int16Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_end <= short.MinValue || _start > short.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                var end = _end > short.MaxValue ? null : (short?)_end;
                ComputeMask(mask, inputArray, (short)Math.Max(_start, short.MinValue), end);
            }
        });
    }

    public void Visit(Int32Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_end <= int.MinValue || _start > int.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                var end = _end > int.MaxValue ? null : (int?)_end;
                ComputeMask(mask, inputArray, (int)Math.Max(_start, int.MinValue), end);
            }
        });
    }

    public void Visit(Int64Array array)
    {
        BuildMask(array, (mask, inputArray) => ComputeMask(mask, inputArray, _start, _end));
    }

    private static void ComputeMask<T, TArray>(byte[] mask, TArray array, T rangeStart, T? rangeEnd)
        where T : struct, IComparable<T>
        where TArray : PrimitiveArray<T>
    {
        if (array.NullCount == 0)
        {
            var values = array.Values;
            for (var i = 0; i < array.Length; ++i)
            {
                var value = values[i];
                BitUtility.SetBit(mask, i, value.CompareTo(rangeStart) >= 0 && (!rangeEnd.HasValue || value.CompareTo(rangeEnd.Value) < 0));
            }
        }
        else
        {
            for (var i = 0; i < array.Length; ++i)
            {
                var value = array.GetValue(i);
                BitUtility.SetBit(
                    mask, i,
                    value.HasValue && value.Value.CompareTo(rangeStart) >= 0 && (!rangeEnd.HasValue || value.Value.CompareTo(rangeEnd.Value) < 0));
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
