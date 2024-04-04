namespace ParquetSharp.Dataset.Filter;

public interface ILogicalStatisticsVisitor<out TOut>
{
    TOut Visit(LogicalStatistics stats);
}

public interface ILogicalStatisticsVisitor<TStats, out TOut> : ILogicalStatisticsVisitor<TOut>
{
    TOut Visit(LogicalStatistics<TStats> stats);
}
