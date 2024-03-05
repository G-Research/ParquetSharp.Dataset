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
        /// <summary>
        /// Add a condition that a string column is equal to a specified value
        /// </summary>
        /// <param name="columnName">The column to add the condition on</param>
        /// <param name="value">The value to filter on</param>
        /// <returns>Updated builder</returns>
        public Builder WithEquality(string columnName, string value)
        {
            _constraints.Add(new Constraint(columnName, new StringInSetConstraint(new [] {value})));
            return this;
        }

        /// <summary>
        /// Add a condition that a string column contains a value in a set of allowed values
        /// </summary>
        /// <param name="columnName">The column to add the condition on</param>
        /// <param name="values">The values to filter on</param>
        /// <returns>Updated builder</returns>
        public Builder WithInSet(string columnName, IReadOnlyList<string> values)
        {
            _constraints.Add(new Constraint(columnName, new StringInSetConstraint(values)));
            return this;
        }

        /// <summary>
        /// Add a condition that an integer typed column is equal to a specified value
        /// </summary>
        /// <param name="columnName">The column to add the condition on</param>
        /// <param name="value">The value to filter on</param>
        /// <returns>Updated builder</returns>
        public Builder WithEquality(string columnName, long value)
        {
            _constraints.Add(new Constraint(columnName, new IntEqualityConstraint(value)));
            return this;
        }

        /// <summary>
        /// Add a condition that an integer typed column is within a specified range
        /// </summary>
        /// <param name="columnName">The column to add the condition on</param>
        /// <param name="start">The first value of the range (inclusive)</param>
        /// <param name="end">The last value of the range (inclusive)</param>
        /// <returns>Updated builder</returns>
        public Builder WithRange(string columnName, long start, long end)
        {
            _constraints.Add(new Constraint(columnName, new IntRangeConstraint(start, end)));
            return this;
        }

        /// <summary>
        /// Create a Filter from the configured builder
        /// </summary>
        /// <returns>The created Filter</returns>
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
    /// <param name="partitionInfo">Column values from the partitioning</param>
    /// <returns>True if the partition should be included</returns>
    public bool IncludePartition(PartitionInformation partitionInfo)
    {
        return _constraints.All(constraint => constraint.Satisfied(partitionInfo));
    }

    private readonly IReadOnlyList<Constraint> _constraints;
}
