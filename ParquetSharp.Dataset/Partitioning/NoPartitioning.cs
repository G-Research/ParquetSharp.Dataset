using Apache.Arrow;
using Array = System.Array;

namespace ParquetSharp.Dataset.Partitioning;

/// <summary>
/// Partitioning strategy where subdirectories are arbitrary and do not add information
/// </summary>
public sealed class NoPartitioning : IPartitioning
{
    public sealed class Factory : IPartitioningFactory
    {
        public IPartitioning Build(string[] paths, Apache.Arrow.Schema? schema = null)
        {
            if (schema != null && schema.FieldsList.Count > 0)
            {
                throw new ArgumentException("Expected an empty partition schema when using no partitioning");
            }
            return new NoPartitioning();
        }
    }

    public Apache.Arrow.Schema Schema => EmptySchema;

    public PartitionInformation Parse(string[] pathComponents)
    {
        return new PartitionInformation(new RecordBatch(EmptySchema, Array.Empty<IArrowArray>(), 1));
    }

    private static readonly Apache.Arrow.Schema EmptySchema = new Apache.Arrow.Schema.Builder().Build();
}
