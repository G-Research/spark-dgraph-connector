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

### SBT

Add this line to your `build.sbt` file to use the latest version:

```sbt
libraryDependencies += "uk.co.gresearch.spark" %% "spark-dgraph-connector" % "[1.0.0,)"
```

### Maven

Add this dependency to your `pom.xml` file to use the latest version:

```xml
<dependency>
  <groupId>uk.co.gresearch.spark</groupId>
  <artifactId>spark-dgraph-connector_2.12</artifactId>
  <version>[1.0.0,)</version>
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
