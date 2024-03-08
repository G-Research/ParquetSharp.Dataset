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
    /// <param name="schema">Optional explicit schema.
    /// If not provided it will be inferred from the partitioning and data file schema</param>
    /// <param name="readerProperties">Optional Parquet reader properties</param>
    /// <param name="arrowReaderProperties">Optional Parquet Arrow reader properties</param>
    public DatasetReader(
        string directory,
        IPartitioningFactory? partitioningFactory = null,
        Apache.Arrow.Schema? schema = null,
        ReaderProperties? readerProperties = null,
        ArrowReaderProperties? arrowReaderProperties = null)
    {
        _directory = directory ?? throw new ArgumentNullException(nameof(directory));
        _readerProperties = readerProperties;
        _arrowReaderProperties = arrowReaderProperties;

        partitioningFactory ??= new NoPartitioning.Factory();
        if (schema == null)
        {
            // Infer both schema and partitioning
            var dataFileSchemaBuilder = new DataFileSchemaBuilder(_readerProperties, _arrowReaderProperties);
            InspectTree(_directory, partitioningFactory, dataFileSchemaBuilder);
            Partitioning = partitioningFactory.Build();
            Schema = MergeSchemas(Partitioning.Schema, dataFileSchemaBuilder.Build());
        }
        else
        {
            Schema = schema;
            InspectTree(_directory, partitioningFactory, null);
            Partitioning = partitioningFactory.Build(schema);
        }
    }

    /// <summary>
    /// Create a new DatasetReader, providing a partitioning scheme
    /// </summary>
    /// <param name="directory">The root directory of the dataset</param>
    /// <param name="partitioning">The partitioning to use</param>
    /// <param name="schema">Optional explicit schema.
    /// If not provided it will be inferred from the partitioning and data file schema</param>
    /// <param name="readerProperties">Optional Parquet reader properties</param>
    /// <param name="arrowReaderProperties">Optional Parquet Arrow reader properties</param>
    public DatasetReader(
        string directory,
        IPartitioning partitioning,
        Apache.Arrow.Schema? schema = null,
        ReaderProperties? readerProperties = null,
        ArrowReaderProperties? arrowReaderProperties = null)
    {
        _directory = directory ?? throw new ArgumentNullException(nameof(directory));
        Partitioning = partitioning ?? throw new ArgumentNullException(nameof(partitioning));
        _readerProperties = readerProperties;
        _arrowReaderProperties = arrowReaderProperties;

        if (schema == null)
        {
            var dataFileSchemaBuilder = new DataFileSchemaBuilder(_readerProperties, _arrowReaderProperties);
            InspectTree(_directory, null, dataFileSchemaBuilder);
            Schema = MergeSchemas(partitioning.Schema, dataFileSchemaBuilder.Build());
        }
        else
        {
            // TODO: Validate partitioning schema is a subst of the specified schema
            Schema = schema;
        }
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
    /// <param name="filter">Optional filter to limit data read</param>
    /// <returns>Dataset data as a table</returns>
    public async Task<Table> ToTable(Filter? filter = null)
    {
        var arrayStream = ToBatches(filter);
        var batches = new List<RecordBatch>();
        while (await arrayStream.ReadNextRecordBatchAsync() is { } batch)
        {
            batches.Add(batch);
        }

        return Table.TableFromRecordBatches(Schema, batches);
    }

    /// <summary>
    /// Read a dataset to an Arrow RecordBatch stream
    /// </summary>
    /// <param name="filter">Optional filter to limit data read</param>
    /// <returns>Dataset data as an IArrowArrayStream</returns>
    public IArrowArrayStream ToBatches(Filter? filter = null)
    {
        return new DatasetStreamReader(
            _directory, Schema, Partitioning, filter, _readerProperties, _arrowReaderProperties);
    }

    private static void InspectTree(
        string directory,
        IPartitioningFactory? partitioningFactory,
        DataFileSchemaBuilder? dataSchemaBuilder)
    {
        // Find the first data file and use it to infer partitioning and/or the data file schema.
        // TODO: Allow using multiple paths, in case subtrees do not all have the same structure
        // or data files have different fields?
        // May want to use multiple paths for partitioning but only one for data files?
        var fragmentEnumerator = new FragmentEnumerator(directory, new NoPartitioning(), filter: null);
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

    private readonly string _directory;
    private readonly ReaderProperties? _readerProperties;
    private readonly ArrowReaderProperties? _arrowReaderProperties;
}
