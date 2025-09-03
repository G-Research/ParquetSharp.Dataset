namespace ParquetSharp.Dataset;

/// <summary>
/// Options that control behaviour when reading a Dataset,
/// which aren't specific to a partitioning scheme or Parquet reading.
/// </summary>
public class DatasetOptions
{
    /// <summary>
    /// Files and directories matching these prefixes will be ignored.
    /// Defaults to "." and "_"
    /// </summary>
    public string[] IgnorePrefixes { get; init; } = { ".", "_" };

    /// <summary>
    /// The default DatasetOptions
    /// </summary>
    public static readonly DatasetOptions Default = new();
}
