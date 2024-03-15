using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using ParquetSharp.Arrow;
using ParquetSharp.Dataset.Filter;

namespace ParquetSharp.Dataset;

internal sealed class DatasetStreamReader : IArrowArrayStream
{
    public DatasetStreamReader(
        string directory,
        Apache.Arrow.Schema schema,
        IPartitioning partitioning,
        IFilter? filter = null,
        ReaderProperties? readerProperties = null,
        ArrowReaderProperties? arrowReaderProperties = null)
    {
        Schema = schema;
        _fragmentEnumerator = new FragmentEnumerator(directory, partitioning, filter);
        _fragmentExpander = new FragmentExpander(schema);
        _filter = filter;
        _readerProperties = readerProperties;
        _arrowReaderProperties = arrowReaderProperties;
    }

    public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = new CancellationToken())
    {
        cancellationToken.ThrowIfCancellationRequested();

        GetNextReader();
        while (_currentFragmentReader != null)
        {
            var nextBatch = await _currentFragmentReader.ReadNextRecordBatchAsync(cancellationToken);
            if (nextBatch != null)
            {
                var filtered = FilterBatch(nextBatch);
                return _fragmentExpander.ExpandBatch(
                    filtered, _fragmentEnumerator.Current.PartitionInformation);
            }
            else
            {
                GetNextReader();
            }
        }

        return null;
    }

    private RecordBatch FilterBatch(RecordBatch recordBatch)
    {
        if (_filter == null)
        {
            return recordBatch;
        }

        var filterMask = _filter.ComputeMask(recordBatch);
        if (filterMask == null)
        {
            return recordBatch;
        }

        var arrays = new List<IArrowArray>();
        for (var colIdx = 0; colIdx < recordBatch.ColumnCount; ++colIdx)
        {
            var filterApplier = new ArrayMaskApplier(filterMask);
            recordBatch.Column(colIdx).Accept(filterApplier);
            arrays.Add(filterApplier.MaskedArray);
        }

        return new RecordBatch(recordBatch.Schema, arrays, filterMask.IncludedCount);
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
            var columnIndices = GetFileColumnIndices(_currentFileReader, Schema);
            _currentFragmentReader = _currentFileReader.GetRecordBatchReader(columns: columnIndices);
        }
        else
        {
            _currentFileReader = null;
            _currentFragmentReader = null;
        }
    }

    private static int[] GetFileColumnIndices(FileReader fileReader, Apache.Arrow.Schema schema)
    {
        var fileSchema = fileReader.Schema;
        var columnIndices = new List<int>();
        foreach (var field in schema.FieldsList)
        {
            // Field may come from the partition information rather than the data file
            if (fileSchema.FieldsLookup.Contains(field.Name))
            {
                columnIndices.Add(fileSchema.GetFieldIndex(field.Name));
            }
        }

        return columnIndices.ToArray();
    }

    public Apache.Arrow.Schema Schema { get; }

    private readonly FragmentEnumerator _fragmentEnumerator;
    private readonly IFilter? _filter;
    private readonly ReaderProperties? _readerProperties;
    private readonly ArrowReaderProperties? _arrowReaderProperties;
    private readonly FragmentExpander _fragmentExpander;
    private IArrowArrayStream? _currentFragmentReader = null;
    private FileReader? _currentFileReader = null;
}
