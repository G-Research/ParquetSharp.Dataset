using System.Collections.Generic;
using System.Linq;
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
        _requiredFields = new HashSet<string>(schema.FieldsList.Select(f => f.Name));
        _rowGroupSelector = null;
        if (_filter != null)
        {
            _requiredFields.UnionWith(_filter.Columns());
            _rowGroupSelector = new RowGroupSelector(_filter);
        }
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
                if (filtered == null)
                {
                    // All rows excluded
                    continue;
                }

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

    /// <summary>
    /// Return a record batch with rows filtered out using the current filter.
    /// Returns null if all rows are excluded.
    /// </summary>
    private RecordBatch? FilterBatch(RecordBatch recordBatch)
    {
        if (_filter == null)
        {
            return recordBatch;
        }

        var filterMask = _filter.ComputeMask(recordBatch);
        if (filterMask == null || filterMask.IncludedCount == recordBatch.Length)
        {
            return recordBatch;
        }

        if (filterMask.IncludedCount == 0)
        {
            return null;
        }

        var arrays = new List<IArrowArray>(recordBatch.ColumnCount);
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

            var rowGroups = _rowGroupSelector?.GetRequiredRowGroups(_currentFileReader);
            var columnIndices = GetFileColumnIndices(_currentFileReader);

            _currentFragmentReader = _currentFileReader.GetRecordBatchReader(
                rowGroups: rowGroups, columns: columnIndices);
        }
        else
        {
            _currentFileReader = null;
            _currentFragmentReader = null;
        }
    }

    private int[] GetFileColumnIndices(FileReader fileReader)
    {
        var fileFields = fileReader.Schema.FieldsList;
        var columnIndices = new List<int>(_requiredFields.Count);
        for (var fieldIdx = 0; fieldIdx < fileFields.Count; ++fieldIdx)
        {
            if (_requiredFields.Contains(fileFields[fieldIdx].Name))
            {
                columnIndices.Add(fieldIdx);
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
    private readonly RowGroupSelector? _rowGroupSelector;
    private readonly HashSet<string> _requiredFields;
    private IArrowArrayStream? _currentFragmentReader = null;
    private FileReader? _currentFileReader = null;
}
