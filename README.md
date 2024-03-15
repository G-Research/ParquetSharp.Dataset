# ParquetSharp.Dataset

[![CI Status](https://github.com/G-Research/ParquetSharp.Dataset/actions/workflows/ci.yml/badge.svg?branch=main&event=push)](https://github.com/G-Research/ParquetSharp.Dataset/actions/workflows/ci.yml?query=branch%3Amain+event%3Apush)
[![NuGet latest release](https://img.shields.io/nuget/v/ParquetSharp.Dataset.svg)](https://www.nuget.org/packages/ParquetSharp.Dataset)

**This is a work in progress and is not yet ready for public use**

ParquetSharp.Dataset supports reading datasets consisting of multiple Parquet files,
which may be partitioned with a partitioning strategy such as Hive partitioning.
Data is read using the [Apache Arrow format](https://arrow.apache.org/).

Note that ParquetSharp.Dataset does not use the Apache Arrow C++ Dataset library,
but is implemented on top of [ParquetSharp](https://github.com/G-Research/ParquetSharp),
which uses the Apache Arrow C++ Parquet library.

## Usage

To begin with, you will need a dataset of Parquet files that have the same schema:

```
/my-dataset/data0.parquet
/my-dataset/data1.parquet
```

You can then create a `DatasetReader`, and read data from this as a stream of Arrow `RecordBatch`:

```C#
using ParquetSharp.Dataset;

var dataset = new DatasetReader("/my-dataset");
using var arrayStream = dataset.ToBatches();
while (await reader.ReadNextRecordBatchAsync() is { } batch)
{
    using (batch)
    {
        // Use data in the batch
    }
}
```

Your dataset may be partitioned using Hive partitioning, where directories are named
containing a field name and value:

```
/my-dataset/part=a/data0.parquet
/my-dataset/part=a/data1.parquet
/my-dataset/part=b/data0.parquet
/my-dataset/part=b/data1.parquet
```

To read Hive partitioned data, you can provide a `HivePartitioning.Builder` instance
to the `DatasetReader` constructor, and the partitioning schema will be inferred
by looking at the dataset directory structure:

```C#
var partitioningFactory = new HivePartitioning.Factory();
var dataset = new DatasetReader("/my-dataset", partitioningFactory);
```

Alternatively, you can specify the partitioning schema explicitly:

```C#
var partitioningSchema = new Apache.Arrow.Schema.Builder()
    .Field(new Field("part", new StringType(), nullable: false))
    .Build());
var partitioning = new HivePartitioning(partitioningSchema);
var dataset = new DatasetReader("/my-dataset", partitioning);
```

When creating a `DatasetReader`, the schema from the first Parquet file found will
be inspected to determine the full dataset schema.
This can be avoided by providing the full dataset schema explicitly:

```C#
var datasetSchema = new Apache.Arrow.Schema.Builder()
    .Field(new Field("part", new StringType(), nullable: false))
    .Field(new Field("x", new Int32Type(), nullable: false))
    .Field(new Field("y", new FloatType(), nullable: false))
    .Build());
var dataset = new DatasetReader("/my-dataset", partitioning, datasetSchema);
```

### Filtering data

When reading data from a dataset, you can specify the columns to include
and filter rows based on field values.
Row filters may apply to fields from data files or from the partitioning schema.
When a filter excludes a partition directory no files from that directory
will be read.

```C#
var columns = new[] {"x", "y"};
var filter = Col.Named("part").IsIn(new[] {"a", "c"});
using var arrayStream = dataset.ToBatches(filter, columns);
while (await reader.ReadNextRecordBatchAsync() is { } batch)
{
    using (batch)
    {
        // batch will only contain columns "x" and "y",
        // and only files in the selected partitions will be read.
    }
}
```
