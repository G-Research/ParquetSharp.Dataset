using Apache.Arrow;

namespace ParquetSharp.Dataset;

/// <summary>
/// Holds an Arrow RecordBatch that is guaranteed to have a single row,
/// and contains constant values that apply to all data within a partition.
/// </summary>
public sealed class PartitionInformation
{
    public PartitionInformation(RecordBatch batch)
    {
        if (batch.Length != 1)
        {
            throw new ArgumentException(
                $"Expected a record batch with 1 row, but the length is {batch.Length}", nameof(batch));
        }
        Batch = batch;
    }

    public static PartitionInformation Empty => new PartitionInformation(new RecordBatch(
        new Apache.Arrow.Schema.Builder().Build(), System.Array.Empty<IArrowArray>(), 1));

    public RecordBatch Batch { get; }
}
