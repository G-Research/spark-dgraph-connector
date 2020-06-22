# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased] - YYYY-MM-DD

## [0.3.0] - 2020-06-22

### Added
- Load data from Dgraph cluster as [GraphFrames](https://graphframes.github.io/graphframes/docs/_site/index.html) `GraphFrame`.
- Use exact uid cardinality for uid range partitioning. Combined with predicate partitioning, large
  predicates get split into more partitions than small predicates.
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
