# Spark Dgraph Connector

This projects provides an [Apache Spark](https://spark.apache.org/) connector
for [Dgraph databases](https://dgraph.io/).
It brings various [Spark Data Sources](https://spark.apache.org/docs/latest/sql-data-sources.html)
to read graphs from Dgraph directly into `DataFrame`s or GraphX `Graph`s.

Now, you can do things like:

    val target = "localhost:9080"

    import uk.co.gresearch.spark.dgraph.graphx._
    val graph: Graph[VertexProperty, EdgeProperty] = spark.read.dgraph(target)

    import uk.co.gresearch.spark.dgraph.connector._
    val triples: DataFrame = spark.read.dgraphTriples(target)
    val edges: Dataset[Edge] = spark.read.dgraphEdges(target)
    val nodes: Dataset[TypedNode] = spark.read.dgraphNodes(target)

## Using Spark Dgraph Connector

The Spark Dgraph Connector is available for Spark 2.4 and Spark 3.0, both with Scala 2.12.
Use Maven artifact id `spark-dgraph-connector-2.4_2.12` and `spark-dgraph-connector-3.0_2.12`, respectively.

### SBT

Add this line to your `build.sbt` file to use the latest version:

```sbt
libraryDependencies += "uk.co.gresearch.spark" %% "spark-dgraph-connector-3.0" % "[0,)"
```

### Maven

Add this dependency to your `pom.xml` file to use the latest version:

```xml
<dependency>
  <groupId>uk.co.gresearch.spark</groupId>
  <artifactId>spark-dgraph-connector-3.0_2.12</artifactId>
  <version>[0,)</version>
</dependency>
```

## Examples

The following examples use a local Dgraph instance setup as described in the
[Dgraph Quickstart Guide](https://dgraph.io/docs/get-started).
Run [Step 1](https://dgraph.io/docs/get-started/#step-1-run-dgraph) to start an instance and
[Step 2](https://dgraph.io/docs/get-started/#step-2-run-mutation) to load example graph data and schema:

    ./dgraph-instance.start.sh
    ./dgraph-instance.insert.sh
    ./dgraph-instance.schema.sh

The connection to the Dgraph can be established via a `target`, which is the [hostname and gRPC port of a
Dgraph Alpha node](https://dgraph.io/docs/deploy/#cluster-setup) in the form `"hostname:port"`. With our example instance started above,
we can use `"localhost:9080` as the target.

### GraphX

You can load the entire Dgraph database into a
[Apache Spark GraphX](https://spark.apache.org/docs/latest/graphx-programming-guide.html)
graph:

    import uk.co.gresearch.spark.dgraph.graphx._

    val graph = spark.read.dgraph("localhost:9080")

Perform a [PageRank](https://spark.apache.org/docs/latest/graphx-programming-guide.html#pagerank)
computation on this graph to test the connector:

    val pageRank = graph.pageRank(0.0001)
    pageRank.vertices.foreach(println)


### Dataset

You can load the entire Dgraph database as triples into a [Apache Spark Dataset]().

    import uk.co.gresearch.spark.dgraph.connector._

    val triples = spark.read.dgraphTriples("localhost:9080")

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

|subject|   predicate|objectUid|        objectString|objectLong|objectDouble|    objectTimestamp|objectBoolean|objectGeo|objectPassword|objectType|
|:-----:|:----------:|:-------:|:------------------:|:--------:|:----------:|:-----------------:|:-----------:|:-------:|:------------:|:--------:|
|      1|        name|     null|Star Wars: Episod...|      null|        null|               null|         null|     null|          null|    string|
|      1|    starring|        2|                null|      null|        null|               null|         null|     null|          null|       uid|
|      1|    starring|        3|                null|      null|        null|               null|         null|     null|          null|       uid|
|      1|    starring|        7|                null|      null|        null|               null|         null|     null|          null|       uid|
|      1|running_time|     null|                null|       121|        null|               null|         null|     null|          null|      long|
|      1|release_date|     null|                null|      null|        null|1977-05-25 00:00:00|         null|     null|          null| timestamp|
|      1|    director|        4|                null|      null|        null|               null|         null|     null|          null|       uid|
|      1|     revenue|     null|                null|      null|      7.75E8|               null|         null|     null|          null|    double|
|      1| dgraph.type|     null|                Film|      null|        null|               null|         null|     null|          null|    string|
|      2|        name|     null|      Luke Skywalker|      null|        null|               null|         null|     null|          null|    string|
|      2| dgraph.type|     null|              Person|      null|        null|               null|         null|     null|          null|    string|
|      3|        name|     null|            Han Solo|      null|        null|               null|         null|     null|          null|    string|
|      3| dgraph.type|     null|              Person|      null|        null|               null|         null|     null|          null|    string|
|      4|        name|     null|        George Lucas|      null|        null|               null|         null|     null|          null|    string|
|      4| dgraph.type|     null|              Person|      null|        null|               null|         null|     null|          null|    string|
|      5|        name|     null|     Irvin Kernshner|      null|        null|               null|         null|     null|          null|    string|
|      5| dgraph.type|     null|              Person|      null|        null|               null|         null|     null|          null|    string|

This model allows to store the triples fully typed in a `DataFrame`.

The triples can also be loaded in an un-typed narrow form:

    import uk.co.gresearch.spark.dgraph.connector._

    spark
      .read
      .option(TriplesModeOption, TriplesModeStringOption)
      .dgraphTriples("localhost:9080").show

The returned `DataFrame` has the following schema:

    root
     |-- subject: long (nullable = false)
     |-- predicate: string (nullable = true)
     |-- objectString: string (nullable = true)
     |-- objectType: string (nullable = true)

The object value gets stored as a string in `objectString` and `objectType` provides you
with the actual type of the object. Here is an example:

|subject|   predicate|        objectString|objectType|
|:-----:|:----------:|:------------------:|:--------:|
|      1|        name|Star Wars: Episod...|    string|
|      1|    starring|                   2|       uid|
|      1|    starring|                   3|       uid|
|      1|    starring|                   7|       uid|
|      1|running_time|                 121|      long|
|      1|release_date|1977-05-25 00:00:...| timestamp|
|      1|    director|                   4|       uid|
|      1|     revenue|              7.75E8|    double|
|      1| dgraph.type|                Film|    string|
|      2|        name|      Luke Skywalker|    string|
|      2| dgraph.type|              Person|    string|
|      3|        name|            Han Solo|    string|
|      3| dgraph.type|              Person|    string|
|      4|        name|        George Lucas|    string|
|      4| dgraph.type|              Person|    string|
|      5|        name|     Irvin Kernshner|    string|
|      5| dgraph.type|              Person|    string|

## Partitioning

Partitioning the Dgraph is essential to be able to load large quantities of graph data into Spark.
Spark splits data into partitions, where ideally all partitions have the same size and are of decent size.
Partitions that are too large will kill your Spark executor as they won't fit into memory. When partitions
are too small your Spark jobs becomes inefficient and slow, but will not fail.

Each partition connects to the Dgraph cluster and reads a specific sub-graph. Partitions are non-overlapping.

This connector provides various ways to partition your graph. When the default partitioning does not work for your
specific use case, try a more appropriate partitioning scheme.

### Partitioner

The following `Partitioner` implementations are availabe:

| Partitioner             | partition by | Description | Use Case |
|:-----------------------:|:------------:|-------------|----------|
| Singleton               | _nothing_    | Provides a single partition for the entire graph. | Unit Tests and small graphs that fit into a single partition. Can be used for large graphs if combined with a "by uid" partitioner. |
| Uid Range _(default)_   | uids         | Each partition has at most `N` uids. | Large graphs where single `uid`s fit into a partition. Can be combined with any "by predicate" partitioner, otherwise induces Dgraph cluster internal communication across groups. |
| Predicate               | predicate    | Provides multiple partitions with at most `P` predicates per partition. Partitions by group first (see "Group" partitioner). | Graphs with a large number of different predicates. Each predicate should fit into one partition. Skewness of predicates reflects skewness of partitions. |
| Group                   | predicate    | Provides a single partition for each [Dgraph cluster group](https://dgraph.io/docs/design-concepts/#group). A partition contains only predicates of that group. | Dgraph cluster with multiple groups where number of predicates per group is < 1000. Not very useful on its own but can be combined with `uid` partitioners to avoid Dgraph internal communication. |
| Alpha                   | predicate    | Provides `N` partitions for each [Dgraph cluster alpha](https://dgraph.io/docs/deploy/#cluster-setup). A partition contains a subset of predicates of the alpha's group only. | Like "Predicate" partitioner but scales with the number of alphas, not predicates. Graphs with a large number of different predicates. |


#### Partitioning by Uids

A `uid` represents a node or vertice in Dgraph terminology. A "Uid Range" partitioning splits
the graph by the subject of the graph triples. This can be combined with predicates partitioning,
which serves as an orthogonal partitioning. Without predicate partitioning, `uid` partitioning
induces Dgraph cluster internal communication across the groups.

The space of existing `uids` is split into ranges of `N` `uids` per partition. The `N` defaults to `1000`
and can be configured via `dgraph.partitioner.uidRange.uidsPerPartition`. The `uid`s are allocated to
partitions in ascending order. If vertice size is skewed and a function of `uid`, then partitions will be skewed as well.

#### Partitioning by Predicates

The Dgraph data can be partitioned by predicates. Each partition then contains a distinct set of predicates.
Those partitions connect only to alpha nodes that contain those predicates. Hence, these reads are all
locally to the alpha nodes and induce no Dgraph cluster internal communication.

## Dependencies

The GRPC library used by the dgraph client requires `guava >= 20.0`, hence the

    <dependency>
      <groupId>com.google.guava</groupId>
      <artifactId>guava</artifactId>
      <version>[20.0-jre,)</version>
    </dependency>

in the `pom.xml`file. Otherwise, we would see this error:

      java.lang.NoSuchMethodError: 'void com.google.common.base.Preconditions.checkArgument(boolean, java.lang.String, char, java.lang.Object)'
      at io.grpc.Metadata$Key.validateName(Metadata.java:629)
      at io.grpc.Metadata$Key.<init>(Metadata.java:637)
      at io.grpc.Metadata$Key.<init>(Metadata.java:567)
      at io.grpc.Metadata$AsciiKey.<init>(Metadata.java:742)
      at io.grpc.Metadata$AsciiKey.<init>(Metadata.java:737)
      at io.grpc.Metadata$Key.of(Metadata.java:593)
      at io.grpc.Metadata$Key.of(Metadata.java:589)
      at io.grpc.internal.GrpcUtil.<clinit>(GrpcUtil.java:79)
      at io.grpc.internal.AbstractManagedChannelImplBuilder.<clinit>(AbstractManagedChannelImplBuilder.java:84)
      at uk.co.gresearch.spark.dgraph.connector.package$.toChannel(package.scala:113)

Further, we need to set `protobuf-java >= 3.0.0` in the `pom.xml` file:

    <dependency>
      <groupId>com.google.protobuf</groupId>
      <artifactId>protobuf-java</artifactId>
      <version>[3,]</version>
    </dependency>

to get rid of this error

      java.lang.NoClassDefFoundError: com/google/protobuf/GeneratedMessageV3
      at java.base/java.lang.ClassLoader.defineClass1(Native Method)
      at java.base/java.lang.ClassLoader.defineClass(ClassLoader.java:1017)
      at java.base/java.security.SecureClassLoader.defineClass(SecureClassLoader.java:174)
      at java.base/jdk.internal.loader.BuiltinClassLoader.defineClass(BuiltinClassLoader.java:800)
      at java.base/jdk.internal.loader.BuiltinClassLoader.findClassOnClassPathOrNull(BuiltinClassLoader.java:698)
      at java.base/jdk.internal.loader.BuiltinClassLoader.loadClassOrNull(BuiltinClassLoader.java:621)
      at java.base/jdk.internal.loader.BuiltinClassLoader.loadClass(BuiltinClassLoader.java:579)
      at java.base/jdk.internal.loader.ClassLoaders$AppClassLoader.loadClass(ClassLoaders.java:178)
      at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:522)
      at io.dgraph.AsyncTransaction.<init>(AsyncTransaction.java:48)
      ...
      Cause: java.lang.ClassNotFoundException: com.google.protobuf.GeneratedMessageV3
      at java.base/jdk.internal.loader.BuiltinClassLoader.loadClass(BuiltinClassLoader.java:581)
      at java.base/jdk.internal.loader.ClassLoaders$AppClassLoader.loadClass(ClassLoaders.java:178)
      at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:522)
      at java.base/java.lang.ClassLoader.defineClass1(Native Method)
      at java.base/java.lang.ClassLoader.defineClass(ClassLoader.java:1017)
      at java.base/java.security.SecureClassLoader.defineClass(SecureClassLoader.java:174)
      at java.base/jdk.internal.loader.BuiltinClassLoader.defineClass(BuiltinClassLoader.java:800)
      at java.base/jdk.internal.loader.BuiltinClassLoader.findClassOnClassPathOrNull(BuiltinClassLoader.java:698)
      at java.base/jdk.internal.loader.BuiltinClassLoader.loadClassOrNull(BuiltinClassLoader.java:621)
      at java.base/jdk.internal.loader.BuiltinClassLoader.loadClass(BuiltinClassLoader.java:579)
