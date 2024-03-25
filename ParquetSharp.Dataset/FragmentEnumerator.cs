using System.Collections;

namespace ParquetSharp.Dataset;

/// <summary>
/// A single data file within a dataset
/// </summary>
internal sealed class PartitionFragment
{
    public PartitionFragment(string filePath, PartitionInformation partitionInformation, string[] partitionPath)
    {
        FilePath = filePath;
        PartitionInformation = partitionInformation;
        PartitionPath = partitionPath;
    }

    /// <summary>
    /// Path to the data file
    /// </summary>
    public string FilePath { get; }

    /// <summary>
    /// Array of dataset subdirectory names representing the path to the partition directory
    /// </summary>
    public string[] PartitionPath { get; }

    /// <summary>
    /// Contains field values from the partitioning
    /// </summary>
    public PartitionInformation PartitionInformation { get; }
}

/// <summary>
/// Enumerator over data files in a dataset directory
/// </summary>
internal sealed class FragmentEnumerator : IEnumerator<PartitionFragment>
{
    public FragmentEnumerator(
        string directory,
        IPartitioning partitioning,
        IFilter? filter = null)
    {
        _rootDirectory = directory;
        _filter = filter;
        _partitioning = partitioning;
        _directoryQueue = new Queue<string[]>();
        _directoryQueue.Enqueue(Array.Empty<string>());
        _fileQueue = new Queue<string>();
    }

    public bool MoveNext()
    {
        while (_directoryQueue.Count > 0 && _fileQueue.Count == 0)
        {
            var pathComponents = _directoryQueue.Dequeue();
            var partitionInfo = _partitioning.Parse(pathComponents);
            var includePartition = _filter?.IncludePartition(partitionInfo) ?? true;
            if (!includePartition)
            {
                continue;
            }

            _currentPartition = partitionInfo;
            _currentPartitionPath = pathComponents;

            var directoryPath = Path.Join(_rootDirectory, Path.Join(pathComponents));
            var directoryInfo = new DirectoryInfo(directoryPath);
            foreach (var fsi in directoryInfo.GetFileSystemInfos())
            {
                if ((fsi.Attributes & FileAttributes.Directory) == FileAttributes.Directory)
                {
                    // Subdirectory
                    var subdirectoryComponents = new string[pathComponents.Length + 1];
                    Array.Copy(pathComponents, subdirectoryComponents, pathComponents.Length);
                    subdirectoryComponents[pathComponents.Length] = fsi.Name;
                    _directoryQueue.Enqueue(subdirectoryComponents);
                }
                else
                {
                    // File
                    if (fsi.Extension.Equals(".parquet", StringComparison.OrdinalIgnoreCase))
                    {
                        _fileQueue.Enqueue(fsi.FullName);
                    }
                }
            }
        }

        if (_fileQueue.Count > 0)
        {
            _currentPath = _fileQueue.Dequeue();
            return true;
        }

        return false;
    }

    public void Reset()
    {
        throw new NotSupportedException();
    }

    public PartitionFragment Current
    {
        get
        {
            if (_currentPath == null)
            {
                throw new InvalidOperationException("Enumerator has not been moved yet");
            }

            return new PartitionFragment(_currentPath, _currentPartition!, _currentPartitionPath!);
        }
    }

    object IEnumerator.Current => Current;

    public void Dispose()
    {
    }

    private readonly IFilter? _filter;
    private readonly IPartitioning _partitioning;
    private readonly string _rootDirectory;
    private readonly Queue<string[]> _directoryQueue;
    private readonly Queue<string> _fileQueue;
    private string? _currentPath = null;
    private PartitionInformation? _currentPartition = null;
    private string[]? _currentPartitionPath = null;
}
