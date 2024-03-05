using Apache.Arrow;

namespace ParquetSharp.Dataset;

internal class Constraint
{
    public Constraint(string columnName, IConstraintEvaluator evaluator)
    {
        _columnName = columnName;
        _evaluator = evaluator;
    }

    public bool Satisfied(PartitionInformation partitionInfo)
    {
        if (partitionInfo.Batch.Schema.FieldsLookup.Contains(_columnName))
        {
            var scalarArray = partitionInfo.Batch.Column(_columnName);
            scalarArray.Accept(_evaluator);
            return _evaluator.Satisfied;
        }

        // Column not in the partition data, assume the constraint may be satisfied
        // if we're evaluating a partial dataset path.
        return true;
    }

    private readonly string _columnName;
    private readonly IConstraintEvaluator _evaluator;
}
