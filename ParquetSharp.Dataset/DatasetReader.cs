﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using ParquetSharp.Arrow;
using ParquetSharp.Dataset.Partitioning;

namespace ParquetSharp.Dataset;

/// <summary>
/// Reads Parquet data stored in multiple Parquet files and possibly partitioned according to a partitioning scheme
/// </summary>
public sealed class DatasetReader
{
    /// <summary>
    /// Create a new DatasetReader, providing a factory for inferring partitioning from the directory structure
    /// </summary>
    /// <param name="directory">The root directory of the dataset</param>
    /// <param name="partitioningFactory">Factory for inferring partitioning</param>
    /// <param name="schema">Optional dataset schema.
    /// If not provided it will be inferred from the partitioning and data file schema</param>
    /// <param name="readerProperties">Optional Parquet reader properties</param>
    /// <param name="arrowReaderProperties">Optional Parquet Arrow reader properties</param>
    /// <param name="options">Dataset reading options</param>
    public DatasetReader(
        string directory,
        IPartitioningFactory? partitioningFactory = null,
        Apache.Arrow.Schema? schema = null,
        ReaderProperties? readerProperties = null,
        ArrowReaderProperties? arrowReaderProperties = null,
        DatasetOptions? options = null)
    {
        _directory = directory ?? throw new ArgumentNullException(nameof(directory));
        _readerProperties = readerProperties;
        _arrowReaderProperties = arrowReaderProperties;
        _options = options ?? DatasetOptions.Default;

        partitioningFactory ??= new NoPartitioning.Factory();
        if (schema == null)
        {
            // Infer both schema and partitioning
            var dataFileSchemaBuilder = new DataFileSchemaBuilder(_readerProperties, _arrowReaderProperties);
            InspectTree(_directory, _options, partitioningFactory, dataFileSchemaBuilder);
            Partitioning = partitioningFactory.Build();
            Schema = MergeSchemas(Partitioning.Schema, dataFileSchemaBuilder.Build());
        }
        else
        {
            Schema = schema;
            InspectTree(_directory, _options, partitioningFactory, null);
            Partitioning = partitioningFactory.Build(schema);
        }
    }

    // 0.3.1 Backwards compatibility overload
    public DatasetReader(
        string directory,
        IPartitioningFactory? partitioningFactory,
        Apache.Arrow.Schema? schema,
        ReaderProperties? readerProperties,
        ArrowReaderProperties? arrowReaderProperties)
        : this(directory, partitioningFactory, schema, readerProperties, arrowReaderProperties, null)
    {
    }

    /// <summary>
    /// Create a new DatasetReader, providing a partitioning scheme
    /// </summary>
    /// <param name="directory">The root directory of the dataset</param>
    /// <param name="partitioning">The partitioning to use</param>
    /// <param name="schema">Optional dataset schema.
    /// If not provided it will be inferred from the partitioning and data file schema</param>
    /// <param name="readerProperties">Optional Parquet reader properties</param>
    /// <param name="arrowReaderProperties">Optional Parquet Arrow reader properties</param>
    /// <param name="options">Dataset reading options</param>
    public DatasetReader(
        string directory,
        IPartitioning partitioning,
        Apache.Arrow.Schema? schema = null,
        ReaderProperties? readerProperties = null,
        ArrowReaderProperties? arrowReaderProperties = null,
        DatasetOptions? options = null)
    {
        _directory = directory ?? throw new ArgumentNullException(nameof(directory));
        Partitioning = partitioning ?? throw new ArgumentNullException(nameof(partitioning));
        _readerProperties = readerProperties;
        _arrowReaderProperties = arrowReaderProperties;
        _options = options ?? DatasetOptions.Default;

        if (schema == null)
        {
            var dataFileSchemaBuilder = new DataFileSchemaBuilder(_readerProperties, _arrowReaderProperties);
            InspectTree(_directory, _options, null, dataFileSchemaBuilder);
            Schema = MergeSchemas(partitioning.Schema, dataFileSchemaBuilder.Build());
        }
        else
        {
            ValidatePartitionSchema(partitioning.Schema, schema);
            Schema = schema;
        }
    }

    // 0.3.1 Backwards compatibility overload
    public DatasetReader(
        string directory,
        IPartitioning partitioning,
        Apache.Arrow.Schema? schema,
        ReaderProperties? readerProperties,
        ArrowReaderProperties? arrowReaderProperties)
        : this(directory, partitioning, schema, readerProperties, arrowReaderProperties, null)
    {
    }

    /// <summary>
    /// The Arrow Schema for data in this Dataset
    /// </summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>
    /// The Partitioning scheme for this Dataset
    /// </summary>
    public IPartitioning Partitioning { get; }

    /// <summary>
    /// Read a dataset to an Arrow Table
    /// </summary>
    /// <param name="filter">Optional filter to limit rows read</param>
    /// <param name="columns">Optional list of names of columns to read</param>
    /// <param name="excludeColumns">Optional list of names of columns to exclude.
    /// Cannot be set if columns is also set. If neither is set then all columns are read.</param>
    /// <returns>Selected dataset data as a table</returns>
    public async Task<Table> ToTable(
        IFilter? filter = null,
        IReadOnlyCollection<string>? columns = null,
        IReadOnlyCollection<string>? excludeColumns = null)
    {
        var arrayStream = ToBatches(filter, columns: columns, excludeColumns: excludeColumns);
        var batches = new List<RecordBatch>();
        while (await arrayStream.ReadNextRecordBatchAsync() is { } batch)
        {
            batches.Add(batch);
        }

        return Table.TableFromRecordBatches(arrayStream.Schema, batches);
    }

    /// <summary>
    /// Read a dataset to an Arrow RecordBatch stream
    /// </summary>
    /// <param name="filter">Optional filter to limit rows read</param>
    /// <param name="columns">Optional list of names of columns to read</param>
    /// <param name="excludeColumns">Optional list of names of columns to exclude.
    /// Cannot be set if columns is also set. If neither is set then all columns are read.</param>
    /// <returns>Selected dataset data as an IArrowArrayStream</returns>
    public IArrowArrayStream ToBatches(
        IFilter? filter = null,
        IReadOnlyCollection<string>? columns = null,
        IReadOnlyCollection<string>? excludeColumns = null)
    {
        if (columns != null && excludeColumns != null)
        {
            throw new Exception("Cannot specify both columns and excludeColumns");
        }

        if (filter != null)
        {
            foreach (var column in filter.Columns())
            {
                if (!Schema.FieldsLookup.Contains(column))
                {
                    throw new ArgumentException(
                        $"Invalid field name '{column}' in filter expression", nameof(filter));
                }
            }
        }

        Apache.Arrow.Schema schema;
        if (columns != null)
        {
            var schemaBuilder = new Apache.Arrow.Schema.Builder();
            foreach (var columnName in columns)
            {
                var field = Schema.GetFieldByName(columnName);
                if (field == null)
                {
                    throw new ArgumentException($"Invalid column name '{columnName}'", nameof(columns));
                }

                schemaBuilder.Field(field);
            }

            schema = schemaBuilder.Build();
        }
        else if (excludeColumns != null)
        {
            var schemaBuilder = new Apache.Arrow.Schema.Builder();
            var excludeColumnsSet = new HashSet<string>(excludeColumns.Count, StringComparer.Ordinal);
            foreach (var excludeColumn in excludeColumns)
            {
                if (!Schema.FieldsLookup.Contains(excludeColumn))
                {
                    throw new ArgumentException(
                        $"Invalid column name '{excludeColumn}' in excluded columns", nameof(excludeColumns));
                }

                excludeColumnsSet.Add(excludeColumn);
            }

            foreach (var field in Schema.FieldsList)
            {
                if (!excludeColumnsSet.Contains(field.Name))
                {
                    schemaBuilder.Field(field);
                }
            }

            schema = schemaBuilder.Build();
        }
        else
        {
            schema = Schema;
        }

        return new DatasetStreamReader(
            _directory, schema, Partitioning, _options, filter, _readerProperties, _arrowReaderProperties);
    }

    private static void InspectTree(
        string directory,
        DatasetOptions options,
        IPartitioningFactory? partitioningFactory,
        DataFileSchemaBuilder? dataSchemaBuilder)
    {
        // Find the first data file and use it to infer partitioning and/or the data file schema.
        // TODO: Allow using multiple paths, in case subtrees do not all have the same structure
        // or data files have different fields? May also need this for handling nullable partition
        // fields if the first path contains a null.
        // May want to use multiple paths for partitioning but only one for data files?
        var fragmentEnumerator = new FragmentEnumerator(directory, new NoPartitioning(), options, filter: null);
        if (fragmentEnumerator.MoveNext())
        {
            partitioningFactory?.Inspect(fragmentEnumerator.Current.PartitionPath);
            dataSchemaBuilder?.Inspect(fragmentEnumerator.Current.FilePath);
        }
    }

    private static Apache.Arrow.Schema MergeSchemas(
        Apache.Arrow.Schema partitioningSchema,
        Apache.Arrow.Schema dataSchema)
    {
        if (partitioningSchema.FieldsList.Count == 0)
        {
            return dataSchema;
        }

        var builder = new Apache.Arrow.Schema.Builder();
        var partitionFields = new HashSet<string>();
        foreach (var field in partitioningSchema.FieldsList)
        {
            partitionFields.Add(field.Name);
            builder.Field(field);
        }

        foreach (var field in dataSchema.FieldsList)
        {
            if (partitionFields.Contains(field.Name))
            {
                throw new Exception($"Duplicate field name '{field.Name}' found in partition schema and data file schema");
            }

            builder.Field(field);
        }

        // Metadata is currently ignored
        return builder.Build();
    }

    private static void ValidatePartitionSchema(Apache.Arrow.Schema partitioningSchema, Apache.Arrow.Schema datasetSchema)
    {
        foreach (var field in partitioningSchema.FieldsList)
        {
            if (!datasetSchema.FieldsLookup.Contains(field.Name))
            {
                throw new Exception(
                    $"Partitioning field '{field.Name}' is not present in the dataset schema");
            }

            var datasetField = datasetSchema.GetFieldByName(field.Name);
            var typeComparer = new TypeComparer(datasetField.DataType);
            field.DataType.Accept(typeComparer);
            if (!typeComparer.TypesMatch)
            {
                throw new Exception(
                    $"Partitioning field '{field.Name}' type {field.DataType} does not match the dataset field type {datasetField.DataType}");
            }
        }
    }

    private readonly string _directory;
    private readonly ReaderProperties? _readerProperties;
    private readonly ArrowReaderProperties? _arrowReaderProperties;
    private readonly DatasetOptions _options;
}
