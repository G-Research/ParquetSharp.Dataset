using Apache.Arrow;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Tests whether a filter condition is satisfied based on a scalar (length=1) array
/// </summary>
internal interface IFilterEvaluator : IArrowArrayVisitor
{
    bool Satisfied { get; }
}
