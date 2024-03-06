using Apache.Arrow;
using Apache.Arrow.Ipc;
using ParquetSharp.Arrow;

namespace ParquetSharp.Dataset;

internal sealed class DatasetStreamReader : IArrowArrayStream
{
    public DatasetStreamReader(
        string directory,
        Apache.Arrow.Schema schema,
        IPartitioning partitioning,
        Filter? filter = null,
        ReaderProperties? readerProperties = null,
        ArrowReaderProperties? arrowReaderProperties = null)
    {
        Schema = schema;
        _fragmentEnumerator = new FragmentEnumerator(directory, partitioning, filter);
        _fragmentExpander = new FragmentExpander(schema);
        _readerProperties = readerProperties;
        _arrowReaderProperties = arrowReaderProperties;
    }

    public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = new CancellationToken())
    {
        cancellationToken.ThrowIfCancellationRequested();

        while (_currentFragmentReader != null)
        {
            var nextBatch = await _currentFragmentReader.ReadNextRecordBatchAsync(cancellationToken);
            if (nextBatch != null)
            {
                return _fragmentExpander.ExpandBatch(
                    nextBatch, _fragmentEnumerator.Current.PartitionInformation);
            }
            else
            {
                GetNextReader();
            }
        }

        return null;
    }

    public void Dispose()
    {
        _currentFragmentReader?.Dispose();
        _currentFileReader?.Dispose();
    }

    private void GetNextReader()
    {
        _currentFragmentReader?.Dispose();
        _currentFileReader?.Dispose();

        if (_fragmentEnumerator.MoveNext())
        {
            _currentFileReader = new FileReader(
                _fragmentEnumerator.Current.FilePath, _readerProperties, _arrowReaderProperties);
            _currentFragmentReader = _currentFileReader.GetRecordBatchReader();
        }
        else
        {
            _currentFileReader = null;
            _currentFragmentReader = null;
        }
    }

    public Apache.Arrow.Schema Schema { get; }

    private readonly FragmentEnumerator _fragmentEnumerator;
    private readonly ReaderProperties? _readerProperties;
    private readonly ArrowReaderProperties? _arrowReaderProperties;
    private readonly FragmentExpander _fragmentExpander;
    private IArrowArrayStream? _currentFragmentReader = null;
    private FileReader? _currentFileReader = null;
}
