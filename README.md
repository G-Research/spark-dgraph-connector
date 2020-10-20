# Spark Dgraph Connector

This project provides an [Apache Spark](https://spark.apache.org/) connector
for [Dgraph databases](https://dgraph.io/) in Scala and Python.
It comes with a [Spark Data Source](https://spark.apache.org/docs/latest/sql-data-sources.html)
to read graphs from a Dgraph cluster directly into
[DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html),
[GraphX](https://spark.apache.org/docs/latest/graphx-programming-guide.html) or
[GraphFrames](https://graphframes.github.io/graphframes/docs/_site/index.html).
The connector supports [filter pushdown](https://github.com/apache/spark/blob/v3.0.0/sql/catalyst/src/main/java/org/apache/spark/sql/connector/read/SupportsPushDownFilters.java#L30),
[projection pushdown](https://github.com/apache/spark/blob/v3.0.0/sql/catalyst/src/main/java/org/apache/spark/sql/connector/read/SupportsPushDownRequiredColumns.java#L31)
and partitioning by orthogonal dimensions [predicates](#partitioning-by-predicates) and [nodes](#partitioning-by-uids).

Example Scala code:

```scala
import org.apache.spark.sql.DataFrame

val target = "localhost:9080"

import uk.co.gresearch.spark.dgraph.graphx._
val graph: Graph[VertexProperty, EdgeProperty] = spark.read.dgraph.graphx(target)
val edges: RDD[Edge[EdgeProperty]] = spark.read.dgraph.edges(target)
val vertices: RDD[(VertexId, VertexProperty)] = spark.read.dgraph.vertices(target)

import uk.co.gresearch.spark.dgraph.graphframes._
val graph: GraphFrame = spark.read.dgraph.graphframes(target)
val edges: DataFrame = spark.read.dgraph.edges(target)
val vertices: DataFrame = spark.read.dgraph.vertices(target)

import uk.co.gresearch.spark.dgraph.connector._
val triples: DataFrame = spark.read.dgraph.triples(target)
val edges: DataFrame = spark.read.dgraph.edges(target)
val nodes: DataFrame = spark.read.dgraph.nodes(target)
```

Example Python code (pyspark 2.4.2 and ≥3.0, see [PySpark Shell and Python script](#pyspark-shell-and-python-script)):

```python
from pyspark.sql import DataFrame
from gresearch.spark.dgraph.connector import *

triples: DataFrame = spark.read.dgraph.triples("localhost:9080")
edges: DataFrame = spark.read.dgraph.edges("localhost:9080")
nodes: DataFrame = spark.read.dgraph.nodes("localhost:9080")
```

## Limitations

The connector is under continuous development. It has the following known limitations:

- **Read-only**: The connector does not support mutating the graph ([issue #8](https://github.com/G-Research/spark-dgraph-connector/issues/8)).
- **Limited Lifetime of Transactions**: The connector optionally reads all partitions within the same transaction, but concurrent mutations reduce the lifetime of that transaction.
- **Language tags & facets**: The connector cannot read any string values with language tags or facets.

Beside the **language tags & facets**, which is a limitation of Dgraph, all the other issues mentioned
above will be addressed in the near future.

## Using Spark Dgraph Connector

The Spark Dgraph Connector is available for Spark 2.4 and Spark 3.0, both with Scala 2.12.
Use Maven artifact ID `spark-dgraph-connector_2.12`. The Spark version is part of the package version,
e.g. 0.4.2-2.4 and 0.4.2-3.0, respectively.
Minor versions are kept in sync between those two packages, such that identical minor versions contain identical feature sets (where supported by the respective Spark version).

### SBT

Add this line to your `build.sbt` file to use the latest version for Spark 3.0:

```sbt
libraryDependencies += "uk.co.gresearch.spark" %% "spark-dgraph-connector" % "0.4.2-3.0"
```

### Maven

Add this dependency to your `pom.xml` file to use the latest version:

```xml
<dependency>
  <groupId>uk.co.gresearch.spark</groupId>
  <artifactId>spark-dgraph-connector_2.12</artifactId>
  <version>0.4.2-3.0</version>
</dependency>
```

### Spark Shell

Launch the Scala Spark REPL (Spark ≥2.4.0) with the Spark Dgraph Connector dependency (version ≥0.5.0) as follows:

```shell script
spark-shell --packages uk.co.gresearch.spark:spark-dgraph-connector_2.12:0.5.0-3.0
```

### PySpark Shell and Python script

Launch the Python Spark REPL (pyspark 2.4.2 and ≥3.0) with the Spark Dgraph Connector dependency (version ≥0.5.0) as follows:

```shell script
pyspark --packages uk.co.gresearch.spark:spark-dgraph-connector_2.12:0.5.0-3.0
```

Run your Python scripts that use PySpark (pyspark 2.4.2 and ≥3.0) and the Spark Dgraph Connector (version ≥0.5.0) via `spark-submit`:

```shell script
spark-submit --packages uk.co.gresearch.spark:spark-dgraph-connector_2.12:0.5.0-3.0 [script.py]
```

## Examples

The following examples use a local Dgraph (≥20.03.3) instance setup as described in the
[Dgraph Quickstart Guide](https://dgraph.io/docs/get-started).
Run [Step 1](https://dgraph.io/docs/get-started/#step-1-run-dgraph) to start an instance,
a `DROP_ALL` for Dgraph ≥20.07.0 only,
[Step 2](https://dgraph.io/docs/get-started/#step-2-run-mutation) to load example graph data, and
[Step 3](https://dgraph.io/docs/get-started/#step-3-alter-schema) to add a schema. These steps are
provided in the following scripts:

```shell script
./dgraph-instance.start.sh
./dgraph-instance.drop-all.sh  # for Dgraph ≥20.07.0 only
./dgraph-instance.insert.sh
./dgraph-instance.schema.sh
```

The Dgraph version can optionally be set via `DGRAPH_TEST_CLUSTER_VERSION` environment variable.

The connection to Dgraph can be established via a `target`, which is the [hostname and gRPC port of a
Dgraph Alpha node](https://dgraph.io/docs/deploy/#cluster-setup) in the form `<hostname>:<port>`.
With our example instance started above, we can use `localhost:9080` as the target.

### GraphX

You can load the entire Dgraph database into an
[Apache Spark GraphX](https://spark.apache.org/docs/latest/graphx-programming-guide.html)
graph. For example:

```scala
import uk.co.gresearch.spark.dgraph.graphx._

val graph = spark.read.dgraph.graphx("localhost:9080")
```

Example code to perform a [PageRank](https://spark.apache.org/docs/latest/graphx-programming-guide.html#pagerank)
computation on the graph to test that the connector is working:

```scala
val pageRank = graph.pageRank(0.0001)
pageRank.vertices.foreach(println)
```

### GraphFrames

You can load the entire Dgraph database into a
[GraphFrames](https://graphframes.github.io/graphframes/docs/_site/index.html) graph. For example:

```scala
import uk.co.gresearch.spark.dgraph.graphframes._

val graph: GraphFrame = spark.read.dgraph.graphframes("localhost:9080")
```

Example code to perform a [PageRank](https://graphframes.github.io/graphframes/docs/_site/user-guide.html#pagerank)
computation on this graph to test that the connector is working:

```scala
val pageRank = graph.pageRank.maxIter(10)
pageRank.run().triplets.show(false)
```

Note: Predicates get renamed when they are loaded from the Dgraph database. Any `.` (dot) in the name
is replaced by a `_` (underscore). To guarantee uniqueness of names, underscores in the original predicate
names are replaced by two underscores. For instance, predicates `dgraph.type` and `release_date`
become `dgraph_type` and `release__date`, respectively.

### DataFrame

Dgraph data can be loaded into Spark DataFrames in various forms:

- Triples
  - fully typed values
  - string values
- Nodes
  - fully typed properties
  - wide schema
- Edges

#### Typed Triples

You can load the entire Dgraph database as triples into an [Apache Spark DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes). For example:

```scala
import uk.co.gresearch.spark.dgraph.connector._

val triples = spark.read.dgraph.triples("localhost:9080")
```

The returned `DataFrame` has the following schema:

    root
     |-- subject: long (nullable = false)
     |-- predicate: string (nullable = true)
     |-- objectUid: long (nullable = true)
     |-- objectString: string (nullable = true)
     |-- objectLong: long (nullable = true)
     |-- objectDouble: double (nullable = true)
     |-- objectTimestamp: timestamp (nullable = true)
     |-- objectBoolean: boolean (nullable = true)
     |-- objectGeo: string (nullable = true)
     |-- objectPassword: string (nullable = true)
     |-- objectType: string (nullable = true)

The object value gets stored in exactly one of the `object*` (except `objectType`) columns, depending on the type of the value.
The `objectType` column provides the type of the object. Here is an example:

|subject|predicate   |objectString                                  |objectLong|objectDouble|objectTimestamp    |objectBoolean|objectGeo|objectPassword|objectType|
|:-----:|:----------:|:--------------------------------------------:|:--------:|:----------:|:-----------------:|:-----------:|:-------:|:------------:|:--------:|
|1      |dgraph.type |Person                                        |null      |null        |null               |null         |null     |null          |string    |
|1      |name        |Luke Skywalker                                |null      |null        |null               |null         |null     |null          |string    |
|2      |dgraph.type |Person                                        |null      |null        |null               |null         |null     |null          |string    |
|2      |name        |Princess Leia                                 |null      |null        |null               |null         |null     |null          |string    |
|3      |dgraph.type |Film                                          |null      |null        |null               |null         |null     |null          |string    |
|3      |name        |Star Wars: Episode IV - A New Hope            |null      |null        |null               |null         |null     |null          |string    |
|3      |release_date|null                                          |null      |null        |1977-05-25 00:00:00|null         |null     |null          |timestamp |
|3      |revenue     |null                                          |null      |7.75E8      |null               |null         |null     |null          |double    |
|3      |running_time|null                                          |121       |null        |null               |null         |null     |null          |long      |

This model allows you to store the fully-typed triples in a `DataFrame`.

#### String Triples

The triples can also be loaded in an un-typed, narrow form:

```scala
import uk.co.gresearch.spark.dgraph.connector._

spark
  .read
  .option(TriplesModeOption, TriplesModeStringOption)
  .dgraph.triples("localhost:9080")
  .show
```

The resulting `DataFrame` has the following schema:

    root
     |-- subject: long (nullable = false)
     |-- predicate: string (nullable = true)
     |-- objectString: string (nullable = true)
     |-- objectType: string (nullable = true)

The object value gets stored as a string in `objectString`, and `objectType` provides you
with the actual type of the object. Here is an example:

|subject|predicate   |objectString                                  |objectType|
|:-----:|:----------:|:--------------------------------------------:|:--------:|
|1      |dgraph.type |Person                                        |string    |
|1      |name        |Luke Skywalker                                |string    |
|2      |dgraph.type |Person                                        |string    |
|2      |name        |Princess Leia                                 |string    |
|3      |dgraph.type |Film                                          |string    |
|3      |revenue     |7.75E8                                        |double    |
|3      |running_time|121                                           |long      |
|3      |starring    |1                                             |uid       |
|3      |starring    |2                                             |uid       |
|3      |starring    |6                                             |uid       |
|3      |director    |7                                             |uid       |
|3      |name        |Star Wars: Episode IV - A New Hope            |string    |
|3      |release_date|1977-05-25 00:00:00.0                         |timestamp |

#### Typed Nodes

You can load all nodes into a `DataFrame` in a fully-typed form. This contains all the nodes' properties but no edges to other nodes:

```scala
import uk.co.gresearch.spark.dgraph.connector._

spark.read.dgraph.nodes("localhost:9080")
```

The returned `DataFrame` has the following schema:

    root
     |-- subject: long (nullable = false)
     |-- predicate: string (nullable = true)
     |-- objectString: string (nullable = true)
     |-- objectLong: long (nullable = true)
     |-- objectDouble: double (nullable = true)
     |-- objectTimestamp: timestamp (nullable = true)
     |-- objectBoolean: boolean (nullable = true)
     |-- objectGeo: string (nullable = true)
     |-- objectPassword: string (nullable = true)
     |-- objectType: string (nullable = true)

The schema of the returned `DataFrame` is very similar to the typed triples schema, except that there is no `objectUid` column linking to other nodes. Here is an example:

|subject|predicate   |objectString                                  |objectLong|objectDouble|objectTimestamp    |objectBoolean|objectGeo|objectPassword|objectType|
|:-----:|:----------:|:--------------------------------------------:|:--------:|:----------:|:-----------------:|:-----------:|:-------:|:------------:|:--------:|
|1      |dgraph.type |Person                                        |null      |null        |null               |null         |null     |null          |string    |
|1      |name        |Luke Skywalker                                |null      |null        |null               |null         |null     |null          |string    |
|2      |dgraph.type |Person                                        |null      |null        |null               |null         |null     |null          |string    |
|2      |name        |Princess Leia                                 |null      |null        |null               |null         |null     |null          |string    |
|3      |dgraph.type |Film                                          |null      |null        |null               |null         |null     |null          |string    |
|3      |revenue     |null                                          |null      |7.75E8      |null               |null         |null     |null          |double    |
|3      |running_time|null                                          |121       |null        |null               |null         |null     |null          |long      |
|3      |name        |Star Wars: Episode IV - A New Hope            |null      |null        |null               |null         |null     |null          |string    |
|3      |release_date|null                                          |null      |null        |1977-05-25 00:00:00|null         |null     |null          |timestamp |

#### Wide Nodes

Nodes can also be loaded in a wide, fully-typed format:

```scala
import uk.co.gresearch.spark.dgraph.connector._

spark
  .read
  .option(NodesModeOption, NodesModeWideOption)
  .dgraph.nodes("localhost:9080")
```

The returned `DataFrame` has the following schema format, which is dependent on the schema of the underlying Dgraph database.
Node properties are stored in typed columns and are ordered alphabetically (property columns start after the `subject` column):

    root
     |-- subject: long (nullable = false)
     |-- dgraph.graphql.schema: string (nullable = true)
     |-- dgraph.type: string (nullable = true)
     |-- name: string (nullable = true)
     |-- release_date: timestamp (nullable = true)
     |-- revenue: double (nullable = true)
     |-- running_time: long (nullable = true)

Note: The graph schema could become very large and therefore the `DataFrame` could become prohibitively wide.

|subject|dgraph.graphql.schema|dgraph.type|name                                          |release_date       |revenue|running_time|
|:-----:|:-------------------:|:---------:|:--------------------------------------------:|:-----------------:|:-----:|:----------:|
|1      |null                 |Person     |Luke Skywalker                                |null               |null   |null        |
|2      |null                 |Person     |Princess Leia                                 |null               |null   |null        |
|3      |null                 |Film       |Star Wars: Episode IV - A New Hope            |1977-05-25 00:00:00|7.75E8 |121         |
|4      |null                 |Film       |Star Wars: Episode VI - Return of the Jedi    |1983-05-25 00:00:00|5.72E8 |131         |
|5      |null                 |Film       |Star Trek: The Motion Picture                 |1979-12-07 00:00:00|1.39E8 |132         |
|6      |null                 |Person     |Han Solo                                      |null               |null   |null        |
|7      |null                 |Person     |George Lucas                                  |null               |null   |null        |
|8      |null                 |Person     |Irvin Kernshner                               |null               |null   |null        |
|9      |null                 |Person     |Richard Marquand                              |null               |null   |null        |
|10     |null                 |Film       |Star Wars: Episode V - The Empire Strikes Back|1980-05-21 00:00:00|5.34E8 |124         |

Note: The Wide Nodes source enforces the [predicate partitioner](#partitioning-by-predicates) to produce a single partition.

#### Edges

Edges can be loaded as follows:

```scala
import uk.co.gresearch.spark.dgraph.connector._

spark.read.dgraph.edges("localhost:9080")
```

The returned `DataFrame` has the following simple schema:

    root
     |-- subject: long (nullable = false)
     |-- predicate: string (nullable = true)
     |-- objectUid: long (nullable = false)

Though there is only a single `object` column for the destination node, it is called `objectUid` to align with the `DataFrame` schemata above.

|subject|predicate|objectUid|
|:-----:|:-------:|:-------:|
|3      |starring |1        |
|3      |starring |2        |
|3      |starring |6        |
|3      |director |7        |
|4      |starring |1        |
|4      |starring |2        |
|4      |starring |6        |
|4      |director |9        |
|10     |starring |1        |
|10     |starring |2        |
|10     |starring |6        |
|10     |director |8        |

## Transactions

Dgraph isolates reads from writes through transactions. Since the connector initiates multiple reads
while fetching the entire graph (partitioning), writes called [mutations](https://dgraph.io/docs/mutations/)
should be isolated in order to get a consistent snapshot of the graph.

Setting the `dgraph.transaction.mode` option to `"read"` will cause the connector to read all partitions
within the same transaction. However, this will cause an exception on the Dgraph cluster when too many
mutations occur while reading partitions. With that option set to `"none"`, no such exception will
occur but reads are not isolated from writes.

## Filter Pushdown

The connector supports filter pushdown to improve efficiency when reading only sub-graphs.
This is supported only in conjunction with the [predicate partitioner](#partitioning-by-predicates).
Spark filters cannot be pushed for any column and any data source because columns have different meaning.
Columns can be of the following types:

|Column Type|Description|Type |Columns|Sources|
|:---------:|-----------|:---:|:-----:|-------|
|subject column|the subject of the row|`long`|`subject`|all [DataFrame sources](#dataframe)|
|predicate column|the predicate of the row|`string`|`predicate`|all but [Wide Nodes source](#wide-nodes)|
|predicate value column|the value of a specific predicate, column name is predicate name|*any*|one column for each predicate in the schema, e.g. `dgraph.type`|[Wide Nodes source](#wide-nodes)|
|object value columns|object value of the row|`long`<br/>`string`<br/>`long`<br/>`double`<br/>`timestamp`<br/>`boolean`<br/>`geo`<br/>`password`|`objectUid`<br>`objectString`<br>`objectLong`<br>`objectDouble`<br>`objectTimestamp`<br>`objectBoolean`<br>`objectGeo`<br>`objectPassword`|all but [Wide Nodes source](#Wide-nodes)<br>the [String Triples source](#string-triples) has only `objectString`<br>the [Typed Triples source](#typed-triples) lacks the `objectUid`<br>the [Edges source](#edges) has only `objectUid`|
|object type column|the type of the object|`string`|`objectType`|all but [Wide Nodes](#wide-nodes) and [Edges](#edges) source|

The following table lists all supported Spark filters:

|Spark Filter|Supported Columns|Example|
|:----------:|-------|-------|
|`EqualTo`   |<ul><li>subject column</li><li>predicate column</li><li>predicate value column</li><li>object value columns (not for [String Triples source](#string-triples))</li><li>object type column</li></ul>|<ul><li>`.where($"subject" === 1L)`</li><li>`.where($"predicate" === "dgraph.type")`</li><li>`.where($"dgraph.type" === "Person")`</li><li>`.where($"objectLong" === 123)`</li><li>`.where($"objectType" === "string")`</li></ul>|
|`In`        |<ul><li>subject column</li><li>predicate column</li><li>predicate value column</li><li>object value columns (not for [String Triples source](#string-triples))</li><li>object type column</li></ul>|<ul><li>`.where($"subject".isin(1L,2L))`</li><li>`.where($"predicate".isin("release_date", "revenue"))`</li><li>`.where($"dgraph.type".isin("Person","Film"))`</li><li>`.where($"objectLong".isin(123,456))`</li><li>`.where($"objectType".isin("string","long"))`</li></ul>|
|`IsNotNull` |<ul><li>predicate value column</li><li>object value columns (not for [String Triples source](#string-triples))</li></ul>|<ul><li>`.where($"dgraph.type".isNotNull)`</li><li>`.where($"objectLong".isNotNull)`</li></ul>|

## Projection Pushdown

The connector supports projection pushdown to improve efficiency when reading only sub-graphs.
A projection in Spark terms is a `select` operation that selects only a subset of a DataFrame's columns.
The [Wide Nodes source](#wide-nodes) supports projection pushdown on all [predicate value columns](#filter-pushdown).

## Filter and Projection Pushdown Example

The following query uses filter and projection pushdown. First we define a wide node `DataFrame`:

```scala
val df =
  spark.read
    .options(Map(
      NodesModeOption -> NodesModeWideOption,
      PartitionerOption -> PredicatePartitionerOption
    ))
    .dgraph.nodes("localhost:9080")
```

Then we select some columns (projection) and rows (filter):

```scala
df
  .select($"subject", $"`dgraph.type`", $"revenue")  // projection
  .where($"revenue".isNotNull)                       // filter
  .show()
```

This selects the columns `subject`, `dgraph.type` and `revenue` for only those rows that actually have a value for `revenue`.
The underlying query to Dgraph simplifies from (the full graph):

    {
      pred1 as var(func: has(<dgraph.graphql.schema>))
      pred2 as var(func: has(<dgraph.graphql.xid>))
      pred3 as var(func: has(<dgraph.type>))
      pred4 as var(func: has(<name>))
      pred5 as var(func: has(<release_date>))
      pred6 as var(func: has(<revenue>))
      pred7 as var(func: has(<running_time>))

      result (func: uid(pred1,pred2,pred3,pred4,pred5,pred6,pred7)) {
        uid
        <dgraph.graphql.schema>
        <dgraph.graphql.xid>
        <dgraph.type>
        <name>
        <release_date>
        <revenue>
        <running_time>
      }
    }

to (selected predicates and nodes only):

    {
      pred1 as var(func: has(<revenue>))

      result (func: uid(pred1)) {
        uid
        <dgraph.type>
        <revenue>
      }
    }

The response is faster as only relevant data are transfered between Dgraph and Spark.

|subject|dgraph.type|revenue|
|:-----:|:---------:|:-----:|
|      4|       Film| 7.75E8|
|      5|       Film| 5.34E8|
|      6|       Film| 5.72E8|
|      9|       Film| 1.39E8|

## Metrics

The connector (Spark ≥3.0 only) collects metrics per partition that provide insights in throughout and timing of the communication
to the Dgraph cluster. For each request to Dgraph (a chunk), the number of received bytes, uids and retrieval time are recorded and
summed per partition. The values can be seen on the Spark UI for the respective stages that performs the read:

![Dgraph metrics as shown on Spark UI Stages page](static/accumulators.png "Dgraph metrics as shown on Spark UI Stages page")

The connector uses [Spark Accumulators](http://spark.apache.org/docs/1.6.2/api/java/org/apache/spark/Accumulator.html)
to collect these metrics. They can be accessed by the Spark driver via a `SparkListener`:

```scala
val handler = new SparkListener {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit =
    stageCompleted.stageInfo.accumulables.values.foreach(println)
}

spark.sparkContext.addSparkListener(handler)
spark.read.dgraph.triples("localhost:9080").count()
```


The following metrics are available:

|Metric|Description|
|------|-----------|
|`Dgraph Bytes`|Size of JSON responses from the Dgraph cluster in Byte.|
|`Dgraph Chunks`|Number of requests sent to the Dgraph cluster.|
|`Dgraph Time`|Time waited for Dgraph to respond in Seconds.|
|`Dgraph Uids`|Number of Uids read.|


## Partitioning

Partitioning your Dgraph graph is essential to be able to load large quantities of graph data into Spark.
Spark splits data into partitions, where ideally all partitions have the same size and are of decent size.
Partitions that are too large will kill your Spark executor as they won't fit into memory. When partitions
are too small your Spark job becomes inefficient and slow, but will not fail.

Each partition connects to the Dgraph cluster and reads a specific sub-graph. Partitions are non-overlapping.

This connector provides various ways to partition your graph. When the default partitioning does not work for your
specific use case, try a more appropriate partitioning scheme.

### Partitioner

The following `Partitioner` implementations are available:

| Partitioner                       | partition by              | Description | Use Case |
|:---------------------------------:|:-------------------------:|-------------|----------|
| Singleton                         | _nothing_                 | Provides a single partition for the entire graph. | Unit Tests and small graphs that fit into a single partition. Can be used for large graphs if combined with a "by uid" partitioner. |
| Predicate                         | predicate                 | Provides multiple partitions with at most `P` predicates per partition where `P` defaults to `1000`. Picks multiple predicates from the same Dgraph group. | Large graphs where each predicate fits into a partition, otherwise combine with Uid Range partitioner. Skewness of predicates reflects skewness of partitions. |
| Uid Range                         | uids                      | Each partition has at most `N` uids where `N` defaults to `1000000`. | Large graphs where single `uid`s fit into a partition. Can be combined with any predicate partitioner, otherwise induces internal Dgraph cluster communication across groups. |
| Predicate + Uid Range _(default)_ | predicates + uids         | Partitions by predicate first (see Predicate Partitioner), then each partition gets partitioned by uid (see Uid Partitioner) | Graphs of any size. |


#### Partitioning by Predicates

The Dgraph data can be partitioned by predicates. Each partition then contains a distinct set of predicates.
Those partitions connect only to alpha nodes that contain those predicates. Hence, these reads are all
locally to the alpha nodes and induce no Dgraph cluster internal communication.

#### Partitioning by Uids

A `uid` represents a node or vertice in Dgraph terminology. A "Uid Range" partitioning splits
the graph by the subject of the graph triples. This can be combined with predicate partitioning,
which serves as an orthogonal partitioning. Without predicate partitioning, `uid` partitioning
induces internal Dgraph cluster communication across the groups.

The uid partitioning always works on top of a predicate partitioner. If none is defined a singleton partitioner is used.
The number of uids of each underlying partition has to be estimated. Once the number of uids is estimated,
the partition is further split into ranges of that uid space.

The space of existing `uids` is split into ranges of `N` `uids` per partition. The `N` defaults to `1000000`
and can be configured via `dgraph.partitioner.uidRange.uidsPerPartition`. The `uid`s are allocated to
partitions in ascending order. If vertex size is skewed and a function of `uid`, then partitions will be skewed as well.

Note: With uid partitioning, the chunk size configured via `dgraph.chunkSize` should be at least a 10th of
the number of uids per partition configured via `dgraph.partitioner.uidRange.uidsPerPartition` to avoid
inefficiency due to chunks overlapping with partition borders. When your result is sparse w.r.t. the uid space
set the chunk size to 100th or less.

<!-- there is only one estimator left, no need to mention this until we have another
The estimator can be selected with the `dgraph.partitioner.uidRange.estimator` option. These estimators are available:

##### Cluster MaxLeaseId

The Dgraph cluster [maintains a maxLeaseId](https://dgraph.io/docs/deploy/#more-about-state-endpoint), which is the largest possible uid.
It grows as new uids are added to the cluster, so it serves as an upper estimate of the actual largest uid.
Compared to the count estimator it is very cheap to retrieve this value.
This estimator can be selected with the `maxLeaseId` value.
-->

### Streamed Partitions

The connector reads each partition from Dgraph in a streamed fashion. It splits up a partition into smaller chunks,
where each chunk contains `100000` uids. This chunk size can be configured via `dgraph.chunkSize`.
Each chunk sends a single query sent to Dgraph. The chunk size limits the size of the result.
Due to the low memory footprint of the connector, Spark could read your entire graph via a single partition
(you would have to `repartition` the read DataFrame to make Spark shuffle the data properly).
However, this would be would slow, but it proves the connector can handle any size of graph with fixed executor memory requirement.

## Shaded Dependencies

This connector comes packaged with a Java Dgraph client and all its dependencies.
This is necessary because the Dgraph client requires Guava ≥20.0, where ≥24.1.1-jre is recommended,
as well as Protobuf Java ≥3.4.0.
Running this in a Spark deployment would cause these dependencies to conflict with
older versions deployed with Spark. Therefore, these newer versions are provided with
the connector as [shaded dependencies](http://maven.apache.org/plugins/maven-shade-plugin/) (renamed package names).

Please refer to the [NOTICE](NOTICE) file for licences of that third-party software.

## Logging

The connector uses Spark's Log4j standard logging framework. Add the following line to your `log4j.properties` to set
the log level of the connector specifically:

    log4j.logger.uk.co.gresearch.spark.dgraph.connector=DEBUG

See `SPARK_HOME/conf/log4j.properties.template` for a template file.

## Testing

Some unit tests require a Dgraph (≥20.03.3) cluster running at `localhost:9080`. It has to be set up as
described in the [Examples](#examples) section. If that cluster is not running, the unit tests will
launch and set up such a cluster for you. This requires `docker` to be installed on your machine
and will make the tests take longer. If you run those tests frequently it is recommended you run
the cluster setup yourself.

You can set the Dgraph version that is started automatically
by setting environment variable `DGRAPH_TEST_CLUSTER_VERSION`.
The default version is defined in `uk.co.gresearch.spark.dgraph.DgraphTestCluster.DgraphDefaultVersion`.

The Python code can be tested with `pytest`:

```shell script
PYTHONPATH="python:python/test" python -m pytest python/test
```
