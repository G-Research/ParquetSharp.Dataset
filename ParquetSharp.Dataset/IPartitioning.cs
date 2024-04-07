using System.Collections.Generic;

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
    /// <returns>The parsed partition information.</returns>
    PartitionInformation Parse(IReadOnlyList<string> pathComponents);

    /// <summary>
    /// Sort partition directories according to partition field values
    /// </summary>
    /// <param name="parentPath">The path containing the directories</param>
    /// <param name="directoryNames">Array of directory names to be sorted</param>
    void SortDirectories(IReadOnlyList<string> parentPath, string[] directoryNames);
}
