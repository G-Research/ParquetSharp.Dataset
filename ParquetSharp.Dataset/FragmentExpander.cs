using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;

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
        var arrays = new List<IArrowArray>();
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
            else
            {
                // TODO: Set to null or a default value?
                throw new Exception($"Field '{field.Name}' not found in fragment data or partition information");
            }
        }

        return new RecordBatch(_resultSchema, arrays, fragmentBatch.Length);
    }

    private readonly Apache.Arrow.Schema _resultSchema;
}
