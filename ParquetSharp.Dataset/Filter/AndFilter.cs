namespace ParquetSharp.Dataset.Filter;

internal sealed class AndFilter : IFilter
{
    public AndFilter(IFilter first, IFilter second)
    {
        _first = first;
        _second = second;
    }

    public bool IncludePartition(PartitionInformation partitionInformation)
    {
        return _first.IncludePartition(partitionInformation) && _second.IncludePartition(partitionInformation);
    }

    private readonly IFilter _first;
    private readonly IFilter _second;
}
