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
            if (_schema != null)
            {
                return _schema;
            }
            // TODO: Read first file and get schema from partitioning
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Read a dataset to an Arrow Table
    /// </summary>
    /// <param name="filter">Optional filter to limit data read</param>
    /// <returns>Dataset data as a table</returns>
    public Table ToTable(Filter? filter = null)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    /// Read a dataset to an Arrow RecordBatch stream
    /// </summary>
    /// <param name="filter">Optional filter to limit data read</param>
    /// <returns>Dataset data as an IArrowArrayStream</returns>
    public IArrowArrayStream ToBatches(Filter? filter = null)
    {
        throw new NotImplementedException();
    }

    private readonly string _directory;
    private readonly IPartitioning? _partitioning;
    private readonly IPartitioningFactory? _partitioningFactory;
    private readonly Apache.Arrow.Schema? _schema;
    private ReaderProperties? _readerProperties;
    private ArrowReaderProperties? _arrowReaderProperties;
}
