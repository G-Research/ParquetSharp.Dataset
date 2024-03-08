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
        _directory = directory;
        _partitioningFactory = partitioningFactory ?? new NoPartitioning.Factory();
        _schema = schema;
        _readerProperties = readerProperties;
        _arrowReaderProperties = arrowReaderProperties;
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
        _directory = directory;
        _partitioning = partitioning;
        _schema = schema;
        _readerProperties = readerProperties;
        _arrowReaderProperties = arrowReaderProperties;
    }

    /// <summary>
    /// Get the Arrow Schema for data in this Dataset
    /// </summary>
    public Apache.Arrow.Schema Schema
    {
        get
        {
            if (_schema == null)
            {
                _schema = MergeSchemas(Partitioning.Schema, GetDataFileSchema());
            }
            return _schema;
        }
    }

    public IPartitioning Partitioning
    {
        get
        {
            if (_partitioning == null)
            {
                if (_partitioningFactory == null)
                {
                    throw new Exception("Either a partitioning or partitioningFactory must be provided");
                }
                throw new NotImplementedException("Partitioning inference not yet implemented");
            }

            return _partitioning;
        }
    }

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

    private Apache.Arrow.Schema GetDataFileSchema()
    {
        // TODO: Allow reading more than one file or using a specific file, in case some have missing fields?
        var fragmentEnumerator = new FragmentEnumerator(_directory, Partitioning, filter: null);
        if (!fragmentEnumerator.MoveNext())
        {
            // No data files found
            return new Apache.Arrow.Schema.Builder().Build();
        }

        var filePath = fragmentEnumerator.Current.FilePath;
        using var fileReader = new FileReader(filePath, _readerProperties, _arrowReaderProperties);
        return fileReader.Schema;
    }

    private static Apache.Arrow.Schema MergeSchemas(Apache.Arrow.Schema partitioningSchema, Apache.Arrow.Schema dataSchema)
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
    private readonly IPartitioning? _partitioning;
    private readonly IPartitioningFactory? _partitioningFactory;
    private Apache.Arrow.Schema? _schema;
    private readonly ReaderProperties? _readerProperties;
    private readonly ArrowReaderProperties? _arrowReaderProperties;
}
