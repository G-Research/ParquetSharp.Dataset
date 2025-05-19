using System;
using System.Collections.Generic;
using ParquetSharp.Arrow;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Selects row groups to read from a ParquetFile based on the applied filter and
/// row group statistics
/// </summary>
internal sealed class RowGroupSelector
{
    public RowGroupSelector(IFilter filter)
    {
        _filter = filter;
    }

    /// <summary>
    /// Get the indices of row groups to read, or return null to read all row groups
    /// </summary>
    /// <param name="reader">The Arrow file reader to get row groups for</param>
    public int[]? GetRequiredRowGroups(FileReader reader)
    {
        var numRowGroups = reader.NumRowGroups;
        var columnIndices = GetParquetColumnIndices(reader);
        if (columnIndices.Count == 0)
        {
            // Filter fields are not fields in the Parquet file
            return null;
        }

        using var parquetReader = reader.ParquetReader;
        using var fileMetadata = parquetReader.FileMetaData;
        var schemaDescriptor = fileMetadata.Schema;

        var rowGroups = new List<int>(numRowGroups);
        var columnStatistics = new Dictionary<string, LogicalStatistics>(columnIndices.Count);
        for (var rowGroupIdx = 0; rowGroupIdx < numRowGroups; ++rowGroupIdx)
        {
            using var rowGroup = parquetReader.RowGroup(rowGroupIdx);
            foreach (var (columnName, columnIndex) in columnIndices)
            {
                using var metadata = rowGroup.MetaData.GetColumnChunkMetaData(columnIndex);
                using var statistics = metadata.Statistics;
                var logicalStatistics = LogicalStatistics.FromStatistics(
                    statistics, schemaDescriptor.Column(columnIndex));
                if (logicalStatistics != null)
                {
                    columnStatistics[columnName] = logicalStatistics;
                }
                // Else if statistics are not available, we may still be able to exclude this
                // row group based on another column if an "and" condition is used.
            }

            if (_filter.IncludeRowGroup(columnStatistics))
            {
                rowGroups.Add(rowGroupIdx);
            }

            columnStatistics.Clear();
        }

        return rowGroups.ToArray();
    }

    /// <summary>
    /// Get the column indices in the Parquet file corresponding to filter fields
    /// </summary>
    /// <param name="reader">The Arrow file reader</param>
    private Dictionary<string, int> GetParquetColumnIndices(FileReader reader)
    {
        var columnIndices = new Dictionary<string, int>();
        var schema = reader.Schema;
        var manifest = reader.SchemaManifest;
        foreach (var column in _filter.Columns())
        {
            if (schema.FieldsLookup.Contains(column))
            {
                var fieldIndex = schema.GetFieldIndex(column);
                var columnIndex = manifest.SchemaField(fieldIndex).ColumnIndex;
                if (columnIndex >= 0)
                {
                    columnIndices[column] = columnIndex;
                }
                else
                {
                    // This shouldn't happen as FieldLookup only searches top level columns
                    throw new Exception(
                        $"Cannot filter on field {fieldIndex} ('{column}'), which is not a leaf-level Parquet column");
                }
            }
        }

        return columnIndices;
    }

    private readonly IFilter _filter;
}
