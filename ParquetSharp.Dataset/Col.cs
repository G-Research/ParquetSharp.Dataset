namespace ParquetSharp.Dataset;

/// <summary>
/// Represents a column to be used in a filter expression
/// </summary>
public sealed class Col
{
    public static Col Named(string name)
    {
        return new Col(name);
    }

    private Col(string name)
    {
        Name = name;
    }

    public string Name { get; }
}
