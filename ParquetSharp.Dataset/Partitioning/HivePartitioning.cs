using System;
using System.Collections.Generic;
using System.Web;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace ParquetSharp.Dataset.Partitioning;

/// <summary>
/// Implements the Hive partitioning strategy, where directories
/// are named like "columnName=columnValue"
/// </summary>
public sealed class HivePartitioning : IPartitioning
{
    public sealed class Factory : IPartitioningFactory
    {
        public void Inspect(string[] pathComponents)
        {
            foreach (var dirName in pathComponents)
            {
                var (fieldName, fieldValue) = ParseDirectoryName(dirName);
                if (int.TryParse(fieldValue, out _))
                {
                    _observedFields[fieldName] = new Field(fieldName, new Int32Type(), true);
                }
                else
                {
                    _observedFields[fieldName] = new Field(fieldName, new StringType(), true);
                }
            }
        }

        public IPartitioning Build(Apache.Arrow.Schema? schema = null)
        {
            var builder = new Apache.Arrow.Schema.Builder();
            foreach (var field in _observedFields.Values)
            {
                if (schema != null)
                {
                    if (schema.FieldsLookup.Contains(field.Name))
                    {
                        builder.Field(schema.GetFieldByName(field.Name));
                    }
                    else
                    {
                        throw new Exception(
                            $"Found partitioning field '{field.Name}' that is not in the specified schema");
                    }
                }
                else
                {
                    builder.Field(field);
                }
            }

            return new HivePartitioning(builder.Build());
        }

        private readonly Dictionary<string, Field> _observedFields = new();
    }

    public HivePartitioning(Apache.Arrow.Schema schema)
    {
        Schema = schema ?? throw new ArgumentNullException(nameof(schema));
    }

    public Apache.Arrow.Schema Schema { get; }

    public PartitionInformation Parse(string[] pathComponents)
    {
        var arrays = new List<IArrowArray>(pathComponents.Length);
        var fields = new List<Field>(pathComponents.Length);

        foreach (var dirName in pathComponents)
        {
            var (fieldName, fieldValue) = ParseDirectoryName(dirName);
            var field = Schema.GetFieldByName(fieldName);
            if (field == null)
            {
                throw new ArgumentException(
                    $"Invalid field name '{fieldName}' for partitioning", nameof(pathComponents));
            }

            if (fieldValue == HiveNullValueFallback)
            {
                if (!field.IsNullable)
                {
                    throw new ArgumentException(
                        $"Found null value for non-nullable partition field '{fieldName}'", nameof(pathComponents));
                }

                fieldValue = null;
            }

            var parser = new ScalarParser(fieldValue);
            field.DataType.Accept(parser);
            var array = parser.ScalarArray!;

            fields.Add(field);
            arrays.Add(array);
        }

        var schemaBuilder = new Apache.Arrow.Schema.Builder();
        foreach (var field in fields)
        {
            schemaBuilder.Field(field);
        }

        return new PartitionInformation(new RecordBatch(schemaBuilder.Build(), arrays, 1));
    }

    private static (string, string) ParseDirectoryName(string directoryName)
    {
        var split = directoryName.Split('=', 2);
        if (split.Length != 2)
        {
            throw new Exception(
                $"Invalid directory name for Hive partitioning '{directoryName}'");
        }

        var fieldName = HttpUtility.UrlDecode(split[0]);
        var fieldValue = HttpUtility.UrlDecode(split[1]);
        return (fieldName, fieldValue);
    }

    private const string HiveNullValueFallback = "__HIVE_DEFAULT_PARTITION__";
}
