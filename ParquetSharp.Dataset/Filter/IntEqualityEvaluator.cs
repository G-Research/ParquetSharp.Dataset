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
                var expectedValue = (byte)_expectedValue;
                for (var i = 0; i < inputArray.Length; ++i)
                {
                    BitUtility.SetBit(mask, i, inputArray.GetValue(i) == expectedValue);
                }
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
                var expectedValue = (ushort)_expectedValue;
                for (var i = 0; i < inputArray.Length; ++i)
                {
                    BitUtility.SetBit(mask, i, inputArray.GetValue(i) == expectedValue);
                }
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
                var expectedValue = (uint)_expectedValue;
                for (var i = 0; i < inputArray.Length; ++i)
                {
                    BitUtility.SetBit(mask, i, inputArray.GetValue(i) == expectedValue);
                }
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
                var expectedValue = (ulong)_expectedValue;
                for (var i = 0; i < inputArray.Length; ++i)
                {
                    BitUtility.SetBit(mask, i, inputArray.GetValue(i) == expectedValue);
                }
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
                var expectedValue = (sbyte)_expectedValue;
                for (var i = 0; i < inputArray.Length; ++i)
                {
                    BitUtility.SetBit(mask, i, inputArray.GetValue(i) == expectedValue);
                }
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
                var expectedValue = (short)_expectedValue;
                for (var i = 0; i < inputArray.Length; ++i)
                {
                    BitUtility.SetBit(mask, i, inputArray.GetValue(i) == expectedValue);
                }
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
                var expectedValue = (int)_expectedValue;
                for (var i = 0; i < inputArray.Length; ++i)
                {
                    BitUtility.SetBit(mask, i, inputArray.GetValue(i) == expectedValue);
                }
            }
        });
    }

    public void Visit(Int64Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            for (var i = 0; i < inputArray.Length; ++i)
            {
                BitUtility.SetBit(mask, i, inputArray.GetValue(i) == _expectedValue);
            }
        });
    }

    public override void Visit(IArrowArray array)
    {
        throw new NotSupportedException(
            $"Integer equality filter for column '{_columnName}' does not support arrays with type {array.Data.DataType.Name}");
    }

    private readonly long _expectedValue;
    private readonly string _columnName;
}
