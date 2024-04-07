using System;
using System.Collections.Generic;

namespace ParquetSharp.Dataset.Partitioning;

/// <summary>
/// Partitioning strategy where subdirectories are arbitrary and do not add information
/// </summary>
public sealed class NoPartitioning : IPartitioning
{
    public sealed class Factory : IPartitioningFactory
    {
        public void Inspect(IReadOnlyList<string> pathComponents)
        {
        }

        public IPartitioning Build(Apache.Arrow.Schema? schema = null)
        {
            return new NoPartitioning();
        }
    }

    public Apache.Arrow.Schema Schema => EmptySchema;

    public PartitionInformation Parse(IReadOnlyList<string> pathComponents)
    {
        return PartitionInformation.Empty;
    }

    public void SortDirectories(IReadOnlyList<string> parentPath, string[] directoryNames)
    {
        Array.Sort(directoryNames, StringComparer.Ordinal);
    }

    private static readonly Apache.Arrow.Schema EmptySchema = new Apache.Arrow.Schema.Builder().Build();
}
