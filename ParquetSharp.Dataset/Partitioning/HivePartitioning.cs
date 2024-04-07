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
        public void Inspect(IReadOnlyList<string> pathComponents)
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

    private sealed class DirectoryComparer : StringComparer
    {
        public DirectoryComparer(Apache.Arrow.Schema schema)
        {
            _schema = schema;
            _baseComparer = StringComparer.Ordinal;
        }

        public override int Compare(string? x, string? y)
        {
            if (x == null || y == null)
            {
                return _baseComparer.Compare(x, y);
            }

            var (xField, xValue) = ParseDirectoryName(x);
            var (yField, yValue) = ParseDirectoryName(y);
            var fieldComparison = _baseComparer.Compare(xField, yField);
            if (fieldComparison != 0)
            {
                return fieldComparison;
            }

            var field = _schema.GetFieldByName(xField);
            if (field == null)
            {
                throw new Exception($"Invalid field name '{xField}' for partitioning");
            }

            if (field.DataType.IsIntegral() &&
                long.TryParse(xValue, out var xLong) &&
                long.TryParse(yValue, out var yLong))
            {
                return xLong.CompareTo(yLong);
            }

            return _baseComparer.Compare(xValue, yValue);
        }

        public override bool Equals(string? x, string? y)
        {
            throw new NotImplementedException();
        }

        public override int GetHashCode(string obj)
        {
            throw new NotImplementedException();
        }

        private readonly Apache.Arrow.Schema _schema;
        private readonly StringComparer _baseComparer;
    }

    public HivePartitioning(Apache.Arrow.Schema schema)
    {
        Schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _comparer = new DirectoryComparer(schema);
    }

    public Apache.Arrow.Schema Schema { get; }

    public PartitionInformation Parse(IReadOnlyList<string> pathComponents)
    {
        var arrays = new List<IArrowArray>(pathComponents.Count);
        var fields = new List<Field>(pathComponents.Count);

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

    public void SortDirectories(IReadOnlyList<string> parentPath, string[] directoryNames)
    {
        System.Array.Sort(directoryNames, _comparer);
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
    private readonly StringComparer _comparer;
}
