namespace ParquetSharp.Dataset;

public interface IPartitioningFactory
{
    /// <summary>
    /// Visit a subdirectory containing a data file
    /// </summary>
    /// <param name="pathComponents">Array of directory names containing a data file</param>
    void Inspect(string[] pathComponents);

    /// <summary>
    /// Create the partitioning from seen subdirectories
    /// </summary>
    /// <param name="schema">Optional full dataset schema to use, to allow overriding inferred types.</param>
    /// <returns>Partitioning for a Dataset</returns>
    IPartitioning Build(Apache.Arrow.Schema? schema = null);
}
