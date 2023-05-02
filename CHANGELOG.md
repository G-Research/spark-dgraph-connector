# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [UNRELEASED] - YYYY-MM-DD

### Changed
- Estimator "maxLeaseId" renamed to "maxUid", as used with option `dgraph.partitioner.uidRange.estimator`

### Fixed
- Work with maxUid values that cannot be parsed ([pull #216](https://github.com/G-Research/spark-dgraph-connector/pull/216)).
- Handle maxUid values larger than Long.MaxValue ([pull #216](https://github.com/G-Research/spark-dgraph-connector/pull/216)).
- Handle Dgraph data type "default" as plain strings ([pull #223](https://github.com/G-Research/spark-dgraph-connector/pull/223)).
- Supports full unsigned long (64 bits) value range of Dgraph uids, mapped into signed longs ([pull #222](https://github.com/G-Research/spark-dgraph-connector/pull/222)).

## [0.9.0] - 2022-07-14

### Changed
- Moved to shaded Java Dgraph client (uk.co.gresearch.dgraph:dgraph4j-shaded:21.12.0-0).

## [0.8.0] - 2022-01-19

### Changed
- Moved Java Dgraph client to 21.12.0.

### Fixed
- Support latest dgraph release 21.12.0 (#147)

## [0.7.0] - 2021-10-01

### Changed
- Moved Java Dgraph client to 21.03.1.

### Fixed
- Support latest dgraph release 21.03.0 (#101)

## [0.6.0] - 2021-03-05

### Added
- Adds support to read string predicates with language tags like `<http://www.w3.org/2000/01/rdf-schema#label@en>` ([issue #63](https://github.com/G-Research/spark-dgraph-connector/issues/63)).
  This works with any source and mode except the node source in wide mode.
  Note that reading into GraphFrames is based on the wide mode, so only the untagged
  language strings can be read there.
  Filter pushdown is not supported for multi-language predicates yet ([issue #68](https://github.com/G-Research/spark-dgraph-connector/issues/68)).
- Adds readable exception and suggests next steps when GRPC fails with `RESOURCE_EXHAUSTED` code.
- Missing `maxLeaseId` in cluster state response defaults to `1000L` to avoid an exception.

### Changed
- Improves predicate partitioning on projection pushdown as it creates full partitions.
- Fixes bug that did not push predicate value filter correctly down to Dgraph causing incorrect results ([issue #82](https://github.com/G-Research/spark-dgraph-connector/pull/82))
- Fixes bug in reading `geo` and `password` data types.
- Tests against Dgraph 20.03, 20.07 and 20.11.
- Moved Java Dgraph client to 20.11.0.
- Upgraded all dependencies to latest versions.

## [0.5.0] - 2020-10-21

### Added
- Optionally reads all partitions within the same transaction. This guarantees a consistent snapshot of the graph ([issue #6](https://github.com/G-Research/spark-dgraph-connector/issues/6)).
  However, concurrent mutations reduce the lifetime of such a transaction and will cause an exception when lifespan exceeds.
- Add Python API that mirrors the Scala API. The README.md fully documents how to load Dgraph data in PySpark.
- Fixed dependency conflicts between connector dependencies and Spark
  by shading the Java Dgraph client and all its dependencies.

### Changed
- Refactored connector API, renamed `spark.read.dgraph*` methods to `spark.read.dgraph.*`.
- Moved `triples`, `edges` and `nodes` sources from package `uk.co.gresearch.spark.dgraph.connector` to `uk.co.gresearch.spark.dgraph`.
- Moved Java Dgraph client to 20.03.1 and Dgraph test cluster to 20.07.0.

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
- Load data from Dgraph cluster as [GraphFrames](https://graphframes.github.io/graphframes/docs/_site/index.html) `GraphFrame`.
- Use exact uid cardinality for uid range partitioning. Combined with predicate partitioning, large
  predicates get split into more partitions than small predicates ([issue #2](https://github.com/G-Research/spark-dgraph-connector/issues/2)).
- Improve performance of `PredicatePartitioner` for a single predicate per partition (`dgraph.partitioner.predicate.predicatesPerPartition=1`).
  This becomes the new default for this partitioner.
- Move to Spark 3.0.0 release (was 3.0.0-preview2).

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
