using Apache.Arrow;

namespace ParquetSharp.Dataset;

public interface IPartitioning
{
    /// <summary>
    /// The schema of the partitioning. This excludes the schema of files within partition directories
    /// </summary>
    Apache.Arrow.Schema Schema { get; }

    /// <summary>
    /// Parse partition data from a subdirectory path in a dataset
    ///
    /// The path may not be the full path to the leaf level of the data, in which case
    /// the returned partition data should only contain a subset of the full schema fields.
    /// </summary>
    /// <param name="pathComponents">Relative path within a dataset, split into components</param>
    /// <returns>Arrow record batch with parsed partition data. The length of the batch must be 1.</returns>
    RecordBatch Parse(string[] pathComponents);
}
