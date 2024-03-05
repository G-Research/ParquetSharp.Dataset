using Apache.Arrow;

namespace ParquetSharp.Dataset;

internal interface IConstraintEvaluator : IArrowArrayVisitor
{
    bool Satisfied { get; }
}
