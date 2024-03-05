# ParquetSharp.Dataset

Allows reading directories of multiple Parquet files
that may be partitioned with a partitioning strategy such as Hive partitioning.

Note that this is not a wrapper for the Apache Arrow Dataset API,
but reimplements a subset of the Dataset API functionality on top of ParquetSharp.
