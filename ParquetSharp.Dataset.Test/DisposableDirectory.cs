namespace ParquetSharp.Dataset.Test;

internal sealed class DisposableDirectory : IDisposable
{
    public DisposableDirectory()
    {
        _directoryPath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_directoryPath);
    }

    public void Dispose()
    {
        Directory.Delete(_directoryPath, recursive: true);
    }

    public void CreateTree(string[] paths)
    {
        foreach (var path in paths)
        {
            var subdirectory = Path.GetDirectoryName(path);
            if ((subdirectory?.Length ?? 0) > 0)
            {
                subdirectory = Path.Join(_directoryPath, subdirectory);
                if (!Directory.Exists(subdirectory))
                {
                    Directory.CreateDirectory(subdirectory);
                }
            }

            if (!path.EndsWith(Path.DirectorySeparatorChar))
            {
                var fullPath = Path.Join(_directoryPath, path);
                File.WriteAllText(fullPath, "");
            }
        }
    }

    public string DirectoryPath => _directoryPath;

    public string AbsPath(string relativePath)
    {
        var absPath = Path.Join(_directoryPath, relativePath);
        var dir = Path.GetDirectoryName(absPath);
        if (dir != null && !Directory.Exists(dir))
        {
            Directory.CreateDirectory(dir);
        }
        return absPath;
    }

    private readonly string _directoryPath;
}
