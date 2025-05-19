using System;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Compare Timestamp array values using a binary comparison operator
/// </summary>
internal sealed class TimestampComparisonEvaluator :
    BaseFilterEvaluator
    , IArrowArrayVisitor<TimestampArray>
{
    public TimestampComparisonEvaluator(ComparisonOperator op, DateTime value, string columnName)
    {
        _operator = op;
        _value = value;
        _columnName = columnName;
    }

    public void Visit(TimestampArray array)
    {
        var dataType = array.Data.DataType;
        var timestampType = dataType as TimestampType;
        if (timestampType == null)
        {
            throw new Exception(
                $"Expected a TimestampArray to have a TimestampType DataType, got {dataType?.GetType().FullName}");
        }

        BuildMask(array, (mask, inputArray) =>
        {
            var comparisonValue = TimeUtils.ToPrimitiveValue(_value, timestampType.Unit);
            if (inputArray.NullCount == 0)
            {
                var values = inputArray.Values;
                switch (_operator)
                {
                    case ComparisonOperator.Equal:
                    {
                        for (var i = 0; i < inputArray.Length; ++i)
                        {
                            BitUtility.SetBit(mask, i, values[i] == comparisonValue);
                        }

                        break;
                    }
                    case ComparisonOperator.GreaterThan:
                    {
                        for (var i = 0; i < inputArray.Length; ++i)
                        {
                            BitUtility.SetBit(mask, i, values[i] > comparisonValue);
                        }

                        break;
                    }
                    case ComparisonOperator.GreaterThanOrEqual:
                    {
                        for (var i = 0; i < inputArray.Length; ++i)
                        {
                            BitUtility.SetBit(mask, i, values[i] >= comparisonValue);
                        }

                        break;
                    }
                    case ComparisonOperator.LessThan:
                    {
                        for (var i = 0; i < inputArray.Length; ++i)
                        {
                            BitUtility.SetBit(mask, i, values[i] < comparisonValue);
                        }

                        break;
                    }
                    case ComparisonOperator.LessThanOrEqual:
                    {
                        for (var i = 0; i < inputArray.Length; ++i)
                        {
                            BitUtility.SetBit(mask, i, values[i] <= comparisonValue);
                        }

                        break;
                    }
                    default:
                        throw new Exception($"Unexpected comparison operator {_operator}");
                }
            }
            else
            {
                switch (_operator)
                {
                    case ComparisonOperator.Equal:
                    {
                        for (var i = 0; i < inputArray.Length; ++i)
                        {
                            BitUtility.SetBit(mask, i, inputArray.GetValue(i) == comparisonValue);
                        }

                        break;
                    }
                    case ComparisonOperator.GreaterThan:
                    {
                        for (var i = 0; i < inputArray.Length; ++i)
                        {
                            BitUtility.SetBit(mask, i, inputArray.GetValue(i) > comparisonValue);
                        }

                        break;
                    }
                    case ComparisonOperator.GreaterThanOrEqual:
                    {
                        for (var i = 0; i < inputArray.Length; ++i)
                        {
                            BitUtility.SetBit(mask, i, inputArray.GetValue(i) >= comparisonValue);
                        }

                        break;
                    }
                    case ComparisonOperator.LessThan:
                    {
                        for (var i = 0; i < inputArray.Length; ++i)
                        {
                            BitUtility.SetBit(mask, i, inputArray.GetValue(i) < comparisonValue);
                        }

                        break;
                    }
                    case ComparisonOperator.LessThanOrEqual:
                    {
                        for (var i = 0; i < inputArray.Length; ++i)
                        {
                            BitUtility.SetBit(mask, i, inputArray.GetValue(i) <= comparisonValue);
                        }

                        break;
                    }
                    default:
                        throw new Exception($"Unexpected comparison operator {_operator}");
                }
            }
        });
    }

    public override void Visit(IArrowArray array)
    {
        throw new NotSupportedException(
            $"Timestamp comparison filter for column '{_columnName}' does not support arrays with type {array.Data.DataType.Name}");
    }

    private readonly ComparisonOperator _operator;
    private readonly DateTime _value;
    private readonly string _columnName;
}
