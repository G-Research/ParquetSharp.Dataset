using System;
using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Compares array values to a constant using a binary comparison operator
/// </summary>
internal sealed class IntComparisonEvaluator :
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
    public IntComparisonEvaluator(ComparisonOperator op, long value, string columnName)
    {
        _operator = op;
        _value = value;
        _columnName = columnName;
    }

    public void Visit(UInt8Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (ComparisonAlwaysFalse(byte.MinValue, byte.MaxValue))
            {
                mask.AsSpan().Fill(0);
            }
            else if (ComparisonAlwaysTrue(byte.MinValue, byte.MaxValue))
            {
                FillNonNulls(mask, inputArray);
            }
            else
            {
                ComputeMask(mask, inputArray, _operator, (byte)_value);
            }
        });
    }

    public void Visit(UInt16Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (ComparisonAlwaysFalse(ushort.MinValue, ushort.MaxValue))
            {
                mask.AsSpan().Fill(0);
            }
            else if (ComparisonAlwaysTrue(ushort.MinValue, ushort.MaxValue))
            {
                FillNonNulls(mask, inputArray);
            }
            else
            {
                ComputeMask(mask, inputArray, _operator, (ushort)_value);
            }
        });
    }

    public void Visit(UInt32Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (ComparisonAlwaysFalse(uint.MinValue, uint.MaxValue))
            {
                mask.AsSpan().Fill(0);
            }
            else if (ComparisonAlwaysTrue(uint.MinValue, uint.MaxValue))
            {
                FillNonNulls(mask, inputArray);
            }
            else
            {
                ComputeMask(mask, inputArray, _operator, (uint)_value);
            }
        });
    }

    public void Visit(UInt64Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (_value < 0)
            {
                if (_operator == ComparisonOperator.GreaterThan || _operator == ComparisonOperator.GreaterThanOrEqual)
                {
                    FillNonNulls(mask, inputArray);
                }
                else
                {
                    mask.AsSpan().Fill(0);
                }
            }
            else
            {
                ComputeMask(mask, inputArray, _operator, (ulong)_value);
            }
        });
    }

    public void Visit(Int8Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (ComparisonAlwaysFalse(sbyte.MinValue, sbyte.MaxValue))
            {
                mask.AsSpan().Fill(0);
            }
            else if (ComparisonAlwaysTrue(sbyte.MinValue, sbyte.MaxValue))
            {
                FillNonNulls(mask, inputArray);
            }
            else
            {
                ComputeMask(mask, inputArray, _operator, (sbyte)_value);
            }
        });
    }

    public void Visit(Int16Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (ComparisonAlwaysFalse(short.MinValue, short.MaxValue))
            {
                mask.AsSpan().Fill(0);
            }
            else if (ComparisonAlwaysTrue(short.MinValue, short.MaxValue))
            {
                FillNonNulls(mask, inputArray);
            }
            else
            {
                ComputeMask(mask, inputArray, _operator, (short)_value);
            }
        });
    }

    public void Visit(Int32Array array)
    {
        BuildMask(array, (mask, inputArray) =>
        {
            if (ComparisonAlwaysFalse(int.MinValue, int.MaxValue))
            {
                mask.AsSpan().Fill(0);
            }
            else if (ComparisonAlwaysTrue(int.MinValue, int.MaxValue))
            {
                FillNonNulls(mask, inputArray);
            }
            else
            {
                ComputeMask(mask, inputArray, _operator, (int)_value);
            }
        });
    }

    public void Visit(Int64Array array)
    {
        BuildMask(array, (mask, inputArray) => ComputeMask(mask, inputArray, _operator, _value));
    }

    private bool ComparisonAlwaysFalse(long minValue, long maxValue)
    {
        switch (_operator)
        {
            case ComparisonOperator.Equal:
                return _value < minValue || _value > maxValue;
            case ComparisonOperator.GreaterThan:
                return maxValue <= _value;
            case ComparisonOperator.GreaterThanOrEqual:
                return maxValue < _value;
            case ComparisonOperator.LessThan:
                return minValue >= _value;
            case ComparisonOperator.LessThanOrEqual:
                return minValue > _value;
            default:
                throw new Exception($"Unexpected comparison operator {_operator}");
        }
    }

    private bool ComparisonAlwaysTrue(long minValue, long maxValue)
    {
        switch (_operator)
        {
            case ComparisonOperator.Equal:
                return false;
            case ComparisonOperator.GreaterThan:
                return minValue > _value;
            case ComparisonOperator.GreaterThanOrEqual:
                return minValue >= _value;
            case ComparisonOperator.LessThan:
                return maxValue < _value;
            case ComparisonOperator.LessThanOrEqual:
                return maxValue <= _value;
            default:
                throw new Exception($"Unexpected comparison operator {_operator}");
        }
    }

    private static void ComputeMask<T, TArray>(byte[] mask, TArray array, ComparisonOperator op, T comparisonValue)
        where T : struct, IComparable<T>, IEquatable<T>
        where TArray : PrimitiveArray<T>
    {
        if (array.NullCount == 0)
        {
            var values = array.Values;
            switch (op)
            {
                case ComparisonOperator.Equal:
                {
                    for (var i = 0; i < array.Length; ++i)
                    {
                        BitUtility.SetBit(mask, i, values[i].Equals(comparisonValue));
                    }

                    break;
                }
                case ComparisonOperator.GreaterThan:
                {
                    for (var i = 0; i < array.Length; ++i)
                    {
                        BitUtility.SetBit(mask, i, values[i].CompareTo(comparisonValue) > 0);
                    }

                    break;
                }
                case ComparisonOperator.GreaterThanOrEqual:
                {
                    for (var i = 0; i < array.Length; ++i)
                    {
                        BitUtility.SetBit(mask, i, values[i].CompareTo(comparisonValue) >= 0);
                    }

                    break;
                }
                case ComparisonOperator.LessThan:
                {
                    for (var i = 0; i < array.Length; ++i)
                    {
                        BitUtility.SetBit(mask, i, values[i].CompareTo(comparisonValue) < 0);
                    }

                    break;
                }
                case ComparisonOperator.LessThanOrEqual:
                {
                    for (var i = 0; i < array.Length; ++i)
                    {
                        BitUtility.SetBit(mask, i, values[i].CompareTo(comparisonValue) <= 0);
                    }

                    break;
                }
                default:
                {
                    throw new Exception($"Unexpected comparison operator {op}");
                }
            }
        }
        else
        {
            switch (op)
            {
                case ComparisonOperator.Equal:
                {
                    for (var i = 0; i < array.Length; ++i)
                    {
                        BitUtility.SetBit(mask, i, array.GetValue(i).Equals(comparisonValue));
                    }

                    break;
                }
                case ComparisonOperator.GreaterThan:
                {
                    for (var i = 0; i < array.Length; ++i)
                    {
                        var value = array.GetValue(i);
                        BitUtility.SetBit(mask, i, value.HasValue && value.Value.CompareTo(comparisonValue) > 0);
                    }

                    break;
                }
                case ComparisonOperator.GreaterThanOrEqual:
                {
                    for (var i = 0; i < array.Length; ++i)
                    {
                        var value = array.GetValue(i);
                        BitUtility.SetBit(mask, i, value.HasValue && value.Value.CompareTo(comparisonValue) >= 0);
                    }

                    break;
                }
                case ComparisonOperator.LessThan:
                {
                    for (var i = 0; i < array.Length; ++i)
                    {
                        var value = array.GetValue(i);
                        BitUtility.SetBit(mask, i, value.HasValue && value.Value.CompareTo(comparisonValue) < 0);
                    }

                    break;
                }
                case ComparisonOperator.LessThanOrEqual:
                {
                    for (var i = 0; i < array.Length; ++i)
                    {
                        var value = array.GetValue(i);
                        BitUtility.SetBit(mask, i, value.HasValue && value.Value.CompareTo(comparisonValue) <= 0);
                    }

                    break;
                }
                default:
                {
                    throw new Exception($"Unexpected comparison operator {op}");
                }
            }
        }
    }

    private static void FillNonNulls<TArray>(byte[] mask, TArray array)
        where TArray : Apache.Arrow.Array
    {
        if (array.NullCount == 0)
        {
            for (var i = 0; i < array.Length; ++i)
            {
                BitUtility.SetBit(mask, i, true);
            }
        }
        else
        {
            for (var i = 0; i < array.Length; ++i)
            {
                BitUtility.SetBit(mask, i, array.IsValid(i));
            }
        }
    }

    public override void Visit(IArrowArray array)
    {
        throw new NotSupportedException(
            $"Integer comparison filter for column '{_columnName}' does not support arrays with type {array.Data.DataType.Name}");
    }

    private readonly ComparisonOperator _operator;
    private readonly long _value;
    private readonly string _columnName;
}
