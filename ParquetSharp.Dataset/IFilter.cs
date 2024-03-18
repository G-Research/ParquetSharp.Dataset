using System.Collections.Generic;
using Apache.Arrow;
using ParquetSharp.Dataset.Filter;

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

    /// <summary>
    /// Whether to read data for a Parquet row group
    /// </summary>
    /// <param name="columnStatistics">Dictionary of statistics for the filter columns, keyed by name</param>
    /// <returns>True if the row group should be read</returns>
    bool IncludeRowGroup(IReadOnlyDictionary<string, LogicalStatistics> columnStatistics);

    /// <summary>
    /// Compute a boolean mask indicating which rows in a batch should
    /// be included. Can return null to indicate that all rows are included.
    /// </summary>
    /// <param name="dataBatch">The batch of data to be filtered</param>
    /// <returns>Boolean mask where True means a row is included</returns>
    FilterMask? ComputeMask(RecordBatch dataBatch);

    /// <summary>
    /// Get the names of columns required for evaluating the filter
    /// </summary>
    /// <returns>Column names</returns>
    IEnumerable<string> Columns();
}
