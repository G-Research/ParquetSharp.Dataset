namespace ParquetSharp.Dataset;

/// <summary>
/// Selects rows of dataset data to be read
/// </summary>
public interface IFilter
{
    /// <summary>
    /// Whether the partition with the specified column values should be read
    /// </summary>
    /// <param name="partitionInformation">Partition column values</param>
    /// <returns>True if the partition data should be read</returns>
    bool IncludePartition(PartitionInformation partitionInformation);
}
