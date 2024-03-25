using ParquetSharp.Arrow;

namespace ParquetSharp.Dataset;

/// <summary>
/// Determines the schema of data files in a dataset
/// </summary>
internal sealed class DataFileSchemaBuilder
{
    public DataFileSchemaBuilder(ReaderProperties? readerProperties, ArrowReaderProperties? arrowReaderProperties)
    {
        _readerProperties = readerProperties;
        _arrowReaderProperties = arrowReaderProperties;
    }

    public void Inspect(string filePath)
    {
        // We currently assume we'll only inspect a single file schema
        using var fileReader = new FileReader(filePath, _readerProperties, _arrowReaderProperties);
        _schema = fileReader.Schema;
    }

    public Apache.Arrow.Schema Build()
    {
        return _schema ?? new Apache.Arrow.Schema.Builder().Build();
    }

    private readonly ReaderProperties? _readerProperties;
    private readonly ArrowReaderProperties? _arrowReaderProperties;
    private Apache.Arrow.Schema? _schema;
}
