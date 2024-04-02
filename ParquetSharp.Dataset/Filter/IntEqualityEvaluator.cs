using System;
using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Tests whether an array contains a single integer equal to a specified value
/// </summary>
internal sealed class IntEqualityEvaluator :
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
    public IntEqualityEvaluator(long value, string columnName)
    {
        _expectedValue = value;
        _columnName = columnName;
    }

    public void Visit(UInt8Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_expectedValue < 0 || _expectedValue > byte.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                ComputeMask(mask, inputArray, (byte)_expectedValue);
            }
        });
    }

    public void Visit(UInt16Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_expectedValue < 0 || _expectedValue > ushort.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                ComputeMask(mask, inputArray, (ushort)_expectedValue);
            }
        });
    }

    public void Visit(UInt32Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_expectedValue < 0 || _expectedValue > uint.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                ComputeMask(mask, inputArray, (uint)_expectedValue);
            }
        });
    }

    public void Visit(UInt64Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_expectedValue < 0)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                ComputeMask(mask, inputArray, (ulong)_expectedValue);
            }
        });
    }

    public void Visit(Int8Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_expectedValue < sbyte.MinValue || _expectedValue > sbyte.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                ComputeMask(mask, inputArray, (sbyte)_expectedValue);
            }
        });
    }

    public void Visit(Int16Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_expectedValue < short.MinValue || _expectedValue > short.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                ComputeMask(mask, inputArray, (short)_expectedValue);
            }
        });
    }

    public void Visit(Int32Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_expectedValue < int.MinValue || _expectedValue > int.MaxValue)
            {
                mask.AsSpan().Fill(0);
            }
            else
            {
                ComputeMask(mask, inputArray, (int)_expectedValue);
            }
        });
    }

    public void Visit(Int64Array array)
    {
        BuildMask(array, (mask, inputArray) => { ComputeMask(mask, inputArray, _expectedValue); });
    }

    private static void ComputeMask<T, TArray>(byte[] mask, TArray array, T expectedValue)
        where T : struct, IEquatable<T>
        where TArray : PrimitiveArray<T>
    {
        if (array.NullCount == 0)
        {
            var values = array.Values;
            for (var i = 0; i < array.Length; ++i)
            {
                BitUtility.SetBit(mask, i, values[i].Equals(expectedValue));
            }
        }
        else
        {
            for (var i = 0; i < array.Length; ++i)
            {
                BitUtility.SetBit(mask, i, array.GetValue(i).Equals(expectedValue));
            }
        }
    }

    public override void Visit(IArrowArray array)
    {
        throw new NotSupportedException(
            $"Integer equality filter for column '{_columnName}' does not support arrays with type {array.Data.DataType.Name}");
    }

    private readonly long _expectedValue;
    private readonly string _columnName;
}
