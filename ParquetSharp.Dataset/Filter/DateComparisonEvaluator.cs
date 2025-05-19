using System;
using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Compare Date array values using a binary comparison operator
/// </summary>
internal sealed class DateComparisonEvaluator :
    BaseFilterEvaluator
    , IArrowArrayVisitor<Date32Array>
    , IArrowArrayVisitor<Date64Array>
{
    public DateComparisonEvaluator(ComparisonOperator op, DateOnly value, string columnName)
    {
        _operator = op;
        _value = value;
        _columnName = columnName;
    }

    public void Visit(Date32Array array)
    {
        var comparisonValue = _value.DayNumber - ArrowEpoch.DayNumber;
        BuildMask(array, (mask, inputArray) =>
        {
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

    public void Visit(Date64Array array)
    {
        // A Date64Array holds the number of milliseconds since the UNIX epoch,
        // so the conversion from a long value to a date isn't as simple as for Date32
        BuildMask(array, (mask, inputArray) =>
        {
            switch (_operator)
            {
                case ComparisonOperator.Equal:
                {
                    for (var i = 0; i < inputArray.Length; ++i)
                    {
                        BitUtility.SetBit(mask, i, inputArray.GetDateOnly(i) == _value);
                    }

                    break;
                }
                case ComparisonOperator.GreaterThan:
                {
                    for (var i = 0; i < inputArray.Length; ++i)
                    {
                        BitUtility.SetBit(mask, i, inputArray.GetDateOnly(i) > _value);
                    }

                    break;
                }
                case ComparisonOperator.GreaterThanOrEqual:
                {
                    for (var i = 0; i < inputArray.Length; ++i)
                    {
                        BitUtility.SetBit(mask, i, inputArray.GetDateOnly(i) >= _value);
                    }

                    break;
                }
                case ComparisonOperator.LessThan:
                {
                    for (var i = 0; i < inputArray.Length; ++i)
                    {
                        BitUtility.SetBit(mask, i, inputArray.GetDateOnly(i) < _value);
                    }

                    break;
                }
                case ComparisonOperator.LessThanOrEqual:
                {
                    for (var i = 0; i < inputArray.Length; ++i)
                    {
                        BitUtility.SetBit(mask, i, inputArray.GetDateOnly(i) <= _value);
                    }

                    break;
                }
                default:
                    throw new Exception($"Unexpected comparison operator {_operator}");
            }
        });
    }

    public override void Visit(IArrowArray array)
    {
        throw new NotSupportedException(
            $"Date comparison filter for column '{_columnName}' does not support arrays with type {array.Data.DataType.Name}");
    }

    private static readonly DateOnly ArrowEpoch = new DateOnly(1970, 1, 1);

    private readonly ComparisonOperator _operator;
    private readonly DateOnly _value;
    private readonly string _columnName;
}
