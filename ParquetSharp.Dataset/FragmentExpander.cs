using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace ParquetSharp.Dataset;

/// <summary>
/// Adds extra columns to record batches as required,
/// so that their schema matches the requested schema.
/// </summary>
internal sealed class FragmentExpander
{
    public FragmentExpander(Apache.Arrow.Schema resultSchema)
    {
        _resultSchema = resultSchema;
    }

    public RecordBatch ExpandBatch(RecordBatch fragmentBatch, PartitionInformation partitionInfo)
    {
        var fieldCount = _resultSchema.FieldsList.Count;
        var arrays = new List<IArrowArray>(fieldCount);
        var fragmentFields = new HashSet<string>(fragmentBatch.Schema.FieldsList.Select(f => f.Name));
        var partitionFields = new HashSet<string>(partitionInfo.Batch.Schema.FieldsList.Select(f => f.Name));
        for (var i = 0; i < fieldCount; ++i)
        {
            var field = _resultSchema.FieldsList[i];
            if (fragmentFields.Contains(field.Name))
            {
                if (partitionFields.Contains(field.Name))
                {
                    throw new Exception(
                        $"Field '{field.Name}' found in both the fragment data and partition information");
                }

                var typeComparer = new TypeComparer(field.DataType);
                var fragmentField = fragmentBatch.Schema.GetFieldByName(field.Name);
                fragmentField.DataType.Accept(typeComparer);
                if (!typeComparer.TypesMatch)
                {
                    throw new Exception(
                        $"Data type {fragmentField.DataType} for column '{field.Name}' doesn't match the expected type {field.DataType}");
                }

                arrays.Add(fragmentBatch.Column(field.Name));
            }
            else if (partitionFields.Contains(field.Name))
            {
                var arrayCreator = new ConstantArrayCreator(fragmentBatch.Length);
                partitionInfo.Batch.Column(field.Name).Accept(arrayCreator);
                arrays.Add(arrayCreator.Array!);
            }
            else if (field.IsNullable)
            {
                arrays.Add(CreateNullArray(field.DataType, fragmentBatch.Length));
            }
            else
            {
                throw new Exception($"Non-nullable field '{field.Name}' not found in fragment data or partition information");
            }
        }

        return new RecordBatch(_resultSchema, arrays, fragmentBatch.Length);
    }

    private static IArrowArray CreateNullArray(IArrowType dataType, int length)
    {
        var builder = GetArrayBuilder(dataType);
        for (int i = 0; i < length; ++i)
        {
            builder.AppendNull();
        }

        return builder.Build(allocator: null);
    }

    private static IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> GetArrayBuilder(IArrowType dataType)
    {
        return dataType.TypeId switch
        {
            ArrowTypeId.Null => new NullArray.Builder(),
            ArrowTypeId.Boolean => new BooleanArray.Builder(),
            ArrowTypeId.UInt8 => new UInt8Array.Builder(),
            ArrowTypeId.Int8 => new Int8Array.Builder(),
            ArrowTypeId.UInt16 => new UInt16Array.Builder(),
            ArrowTypeId.Int16 => new Int16Array.Builder(),
            ArrowTypeId.UInt32 => new UInt32Array.Builder(),
            ArrowTypeId.Int32 => new Int32Array.Builder(),
            ArrowTypeId.UInt64 => new UInt64Array.Builder(),
            ArrowTypeId.Int64 => new Int64Array.Builder(),
            ArrowTypeId.HalfFloat => new HalfFloatArray.Builder(),
            ArrowTypeId.Float => new FloatArray.Builder(),
            ArrowTypeId.Double => new DoubleArray.Builder(),
            ArrowTypeId.String => new StringArray.Builder(),
            ArrowTypeId.Binary => new BinaryArray.Builder(),
            ArrowTypeId.Date32 => new Date32Array.Builder(),
            ArrowTypeId.Date64 => new Date64Array.Builder(),
            ArrowTypeId.Timestamp => new TimestampArray.Builder(),
            ArrowTypeId.Time32 => new Time32Array.Builder(),
            ArrowTypeId.Time64 => new Time64Array.Builder(),
            ArrowTypeId.Decimal128 => new Decimal128Array.Builder((dataType as Decimal128Type)!),
            ArrowTypeId.Decimal256 => new Decimal256Array.Builder((dataType as Decimal256Type)!),
            ArrowTypeId.List => new ListArray.Builder((dataType as ListType)!.ValueDataType),
            ArrowTypeId.Map => new MapArray.Builder(dataType as MapType),
            ArrowTypeId.FixedSizeList => new FixedSizeListArray.Builder(
                (dataType as FixedSizeListType)!.ValueDataType, (dataType as FixedSizeListType)!.ListSize),
            ArrowTypeId.Duration => new DurationArray.Builder(dataType as DurationType),
            ArrowTypeId.BinaryView => new BinaryViewArray.Builder(),
            ArrowTypeId.StringView => new StringViewArray.Builder(),
            ArrowTypeId.ListView => new ListViewArray.Builder((dataType as ListViewType)!.ValueDataType),
            _ => throw new Exception($"Cannot create an array builder for type {dataType}")
        };
    }

    private readonly Apache.Arrow.Schema _resultSchema;
}
