# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [UNRELEASED] - YYYY-MM-DD

### Fixed
- Fixed dependency conflicts between connector dependencies and Spark.

## [0.4.1] - 2020-07-27

### Added
- Add example how to load Dgraph data in PySpark. Fixed dependency conflicts between connector dependencies and Spark.

## [0.4.0] - 2020-07-24

### Added
- Add Spark filter pushdown and projection pushdown to improve efficiency when loading only subgraphs.
  Filters like `.where($"revenue".isNotNull)` and projections like ``.select($"subject", $"`dgraph.type`", $"revenue")``
  will be pushed to Dgraph and only the relevant graph data will
  be read ([issue #7](https://github.com/G-Research/spark-dgraph-connector/issues/7)).
- Improve performance of `PredicatePartitioner` for multiple predicates per partition. Restoring
  default number of predicates per partition of `1000` from before 0.3.0 ([issue #22](https://github.com/G-Research/spark-dgraph-connector/issues/22)).
- The `PredicatePartitioner` combined with `UidRangePartitioner` is the default partitioner now.
- Add stream-like reading of partitions from Dgraph. Partitions are split into smaller chunks.
  This make Spark read Dgraph partitions of any size.
- Add Dgraph metrics to measure throughput, visible in Spark UI Stages page and through `SparkListener`.

### Security
- Move Google Guava dependency version to 24.1.1-jre due to [known security vulnerability
  fixed in 24.1.1](https://github.com/advisories/GHSA-mvr2-9pj6-7w5j)

## [0.3.0] - 2020-06-22

### Added
- Use exact uid cardinality for uid range partitioning. Combined with predicate partitioning, large
  predicates get split into more partitions than small predicates.
- Improve performance of `PredicatePartitioner` for a single predicate per partition (`dgraph.partitioner.predicate.predicatesPerPartition=1`).
  This becomes the new default for this partitioner.
- Move to Spark 2.4.6 release (was 2.4.5).

### Fixed
- Dgraph groups with no predicates caused a `NullPointerException`.
- Predicate names need to be escaped in Dgraph queries.

## [0.2.0] - 2020-06-11

### Added
- Load nodes from Dgraph cluster as wide nodes (fully typed property columns).
- Added `dgraph.type` and `dgraph.graphql.schema` predicates to be loaded from Dgraph cluster.

## [0.1.0] - 2020-06-09

Initial release of the project

### Added
- Load data from Dgraph cluster as triples (as strings or fully typed), edges or node `DataFrame`s.
- Load data from Dgraph cluster as [Apache Spark GraphX](https://spark.apache.org/docs/latest/graphx-programming-guide.html) `Graph`.
- Partitioning by [Dgraph Group, Alpha node](https://dgraph.io/docs/deploy/#cluster-setup),
  [predicates](https://dgraph.io/docs/tutorial-1/#nodes-and-edges) and
  [uids](https://dgraph.io/docs/tutorial-2/#query-using-uids).
