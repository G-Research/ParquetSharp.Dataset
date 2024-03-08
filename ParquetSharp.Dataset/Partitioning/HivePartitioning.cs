using System.Web;
using Apache.Arrow;

namespace ParquetSharp.Dataset.Partitioning;

/// <summary>
/// Implements the Hive partitioning strategy, where directories
/// are named like "columnName=columnValue"
/// </summary>
public sealed class HivePartitioning : IPartitioning
{
    public HivePartitioning(Apache.Arrow.Schema schema)
    {
        Schema = schema ?? throw new ArgumentNullException(nameof(schema));
    }

    public Apache.Arrow.Schema Schema { get; }

    public PartitionInformation Parse(string[] pathComponents)
    {
        var arrays = new List<IArrowArray>();
        var fields = new List<Field>();

        foreach (var component in pathComponents)
        {
            var split = component.Split('=', 2);
            if (split.Length != 2)
            {
                throw new ArgumentException(
                    $"Invalid directory name for Hive partitioning '{component}'", nameof(pathComponents));
            }

            var fieldName = HttpUtility.UrlDecode(split[0]);
            var fieldValue = HttpUtility.UrlDecode(split[1]);
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

    private const string HiveNullValueFallback = "__HIVE_DEFAULT_PARTITION__";
}
