namespace ParquetSharp.Dataset;

public interface IPartitioningFactory
{
    /// <summary>
    /// Create the partitioning
    /// </summary>
    /// <param name="paths">List of paths found relative to the dataset root, excluding file names</param>
    /// <param name="schema">Optional partitioning schema to use. If provided, the directory structure
    /// will be validated against the schema. Otherwise the schema will be inferred from the directory structure.</param>
    /// <returns>Partitioning for a Dataset</returns>
    IPartitioning Build(string[] paths, Apache.Arrow.Schema? schema = null);
}
