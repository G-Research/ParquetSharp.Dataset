using ParquetSharp.Dataset.Constraints;

namespace ParquetSharp.Dataset;

/// <summary>
/// Defines how to filter data in a dataset based on partition column values
/// </summary>
public sealed class Filter
{
    /// <summary>
    /// Builds a filter expression by combining constraints on partition column values.
    /// Individual constraints are combined with logical AND.
    /// </summary>
    public sealed class Builder
    {
        public Builder WithEquality(string columnName, string value)
        {
            throw new NotImplementedException();
        }

        public Builder WithEquality(string columnName, long value)
        {
            _constraints.Add(new Constraint(columnName, new IntEqualityConstraint(value)));
            return this;
        }

        public Builder WithRange(string columnName, long start, long end)
        {
            throw new NotImplementedException();
        }

        public Filter Build()
        {
            return new Filter(_constraints.ToArray());
        }

        private readonly List<Constraint> _constraints = new();
    }

    private Filter(IReadOnlyList<Constraint> constraints)
    {
        _constraints = constraints;
    }

    /// <summary>
    /// Whether data from a partition should be included
    /// </summary>
    /// <param name="partitionData">RecordBatch with values from the partitioning</param>
    /// <returns>True if the partition should be included</returns>
    public bool IncludePartition(Apache.Arrow.RecordBatch partitionData)
    {
        if (partitionData.Length != 1)
        {
            throw new ArgumentException("Expected partition data with a single row");
        }
        return _constraints.All(constraint => constraint.Satisfied(partitionData));
    }

    private readonly IReadOnlyList<Constraint> _constraints;
}
