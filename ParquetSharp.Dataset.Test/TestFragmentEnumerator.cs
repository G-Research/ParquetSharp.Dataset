using Apache.Arrow;
using Apache.Arrow.Types;
using NUnit.Framework;
using ParquetSharp.Dataset.Partitioning;

namespace ParquetSharp.Dataset.Test;

[TestFixture]
public class TestFragmentEnumerator
{
    [Test]
    public void TestEmptyDirectory()
    {
        using var tmpDir = new DisposableDirectory();
        var enumerator = new FragmentEnumerator(tmpDir.DirectoryPath, new NoPartitioning());

        var fragments = GetAllFragments(enumerator);

        Assert.That(fragments, Is.Empty);
    }

    [Test]
    public void TestNoParquetFiles()
    {
        using var tmpDir = new DisposableDirectory();
        tmpDir.CreateTree(new []
        {
            "a/b/c.txt",
            "d/e.txt",
        });
        var enumerator = new FragmentEnumerator(tmpDir.DirectoryPath, new NoPartitioning());

        var fragments = GetAllFragments(enumerator);

        Assert.That(fragments, Is.Empty);
    }

    [Test]
    public void TestFindsAllParquetFiles()
    {
        using var tmpDir = new DisposableDirectory();
        var paths = new[]
        {
            "a/b/c/data0.parquet",
            "a/b/c/data1.PARQUET",
            "a/b/d/data0.parquet",
            "a/b/d/metadata.json",
            "a/b/e/metadata.json",
            "a/b/data0.parquet",
            "data0.parquet",
            "data1.parquet",
            "data.txt",
        };
        tmpDir.CreateTree(paths);
        var enumerator = new FragmentEnumerator(tmpDir.DirectoryPath, new NoPartitioning());

        var fragments = GetAllFragments(enumerator);

        var filePaths = fragments.Select(f => f.FilePath).ToArray();
        var expectedPaths = paths
            .Where(p => p.EndsWith(".parquet") || p.EndsWith(".PARQUET"))
            .Select(p => Path.Join(tmpDir.DirectoryPath, p)).ToArray();
        Assert.That(filePaths, Is.EquivalentTo(expectedPaths));
        foreach (var fragment in fragments)
        {
            Assert.That(fragment.PartitionInformation.Batch.Schema.FieldsList, Is.Empty);
        }
    }

    [Test]
    public void TestFindsParquetFilesWithPartitioning()
    {
        using var tmpDir = new DisposableDirectory();
        var testData = new Dictionary<string, (int X, int Y)>
        {
            {"x=0/y=0/data0.parquet", (0, 0)},
            {"x=0/y=1/data0.parquet", (0, 1)},
            {"x=1/y=0/data0.parquet", (1, 0)},
            {"x=1/y=1/data0.parquet", (1, 1)},
            {"x=1/y=1/data1.parquet", (1, 1)},
            {"y=0/x=1/data0.parquet", (1, 0)}, // reversed order of fields
        };
        var paths = testData.Keys.ToArray();
        tmpDir.CreateTree(paths);

        var partitioningSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", new Int64Type(), true))
            .Field(new Field("y", new Int64Type(), true))
            .Build();
        var partitioning = new HivePartitioning(partitioningSchema);

        var enumerator = new FragmentEnumerator(tmpDir.DirectoryPath, partitioning);

        var fragments = GetAllFragments(enumerator);

        var filePaths = fragments.Select(f => f.FilePath).ToArray();
        var expectedPaths = paths.Select(p => Path.Join(tmpDir.DirectoryPath, p)).ToArray();
        Assert.That(filePaths, Is.EquivalentTo(expectedPaths));

        foreach (var fragment in fragments)
        {
            var relativePath = Path.GetRelativePath(tmpDir.DirectoryPath, fragment.FilePath);
            var expected = testData[relativePath];
            var partitionBatch = fragment.PartitionInformation.Batch;

            var xArray = partitionBatch.Column("x") as Int64Array;
            Assert.That(xArray, Is.Not.Null);
            Assert.That(xArray!.GetValue(0), Is.EqualTo(expected.X));

            var yArray = partitionBatch.Column("y") as Int64Array;
            Assert.That(yArray, Is.Not.Null);
            Assert.That(yArray!.GetValue(0), Is.EqualTo(expected.Y));
        }
    }

    [Test]
    public void TestFindsParquetFilesWithFilter()
    {
        using var tmpDir = new DisposableDirectory();
        var testData = new Dictionary<string, (int X, int Y, bool Included)>
        {
            {"x=0/y=0/data0.parquet", (0, 0, false)},
            {"x=0/y=1/data0.parquet", (0, 1, true)},
            {"x=1/y=0/data0.parquet", (1, 0, false)},
            {"x=1/y=1/data0.parquet", (1, 1, true)},
            {"x=1/y=1/data1.parquet", (1, 1, true)},
            {"y=0/x=1/data0.parquet", (1, 0, false)},
        };
        var paths = testData.Keys.ToArray();
        tmpDir.CreateTree(paths);

        var partitioningSchema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("x", new Int64Type(), true))
            .Field(new Field("y", new Int64Type(), true))
            .Build();
        var partitioning = new HivePartitioning(partitioningSchema);

        var filter = Col.Named("x").IsInRange(0, 100)
            .And(Col.Named("y").IsEqualTo(1))
            .And(Col.Named("z").IsEqualTo("abc"));

        var enumerator = new FragmentEnumerator(tmpDir.DirectoryPath, partitioning, filter);

        var fragments = GetAllFragments(enumerator);

        var filePaths = fragments.Select(f => f.FilePath).ToArray();
        var expectedPaths = testData
            .Where(kvp => kvp.Value.Included)
            .Select(kvp => Path.Join(tmpDir.DirectoryPath, kvp.Key)).ToArray();
        Assert.That(filePaths, Is.EquivalentTo(expectedPaths));

        foreach (var fragment in fragments)
        {
            var relativePath = Path.GetRelativePath(tmpDir.DirectoryPath, fragment.FilePath);
            var expected = testData[relativePath];
            var partitionBatch = fragment.PartitionInformation.Batch;

            var xArray = partitionBatch.Column("x") as Int64Array;
            Assert.That(xArray, Is.Not.Null);
            Assert.That(xArray!.GetValue(0), Is.EqualTo(expected.X));

            var yArray = partitionBatch.Column("y") as Int64Array;
            Assert.That(yArray, Is.Not.Null);
            Assert.That(yArray!.GetValue(0), Is.EqualTo(expected.Y));
        }
    }

    private static IReadOnlyList<PartitionFragment> GetAllFragments(FragmentEnumerator enumerator)
    {
        var fragments = new List<PartitionFragment>();
        while (enumerator.MoveNext())
        {
            fragments.Add(enumerator.Current);
        }

        return fragments;
    }
}
