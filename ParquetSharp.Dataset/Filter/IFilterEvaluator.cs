using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

internal interface IFilterEvaluator : IArrowArrayVisitor
{
    bool Satisfied { get; }
}
