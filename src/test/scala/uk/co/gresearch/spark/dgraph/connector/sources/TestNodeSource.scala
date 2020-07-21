/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector.sources

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector._

class TestNodeSource extends FunSpec
  with SparkTestSession with DgraphTestCluster
  with FilterPushDownTestHelper {

  import spark.implicits._

  describe("NodeDataSource") {

    lazy val expectedTypedNodes = Set(
      TypedNode(graphQlSchema, "dgraph.type", Some("dgraph.graphql"), None, None, None, None, None, None, "string"),
      TypedNode(graphQlSchema, "dgraph.graphql.xid", Some("dgraph.graphql.schema"), None, None, None, None, None, None, "string"),
      TypedNode(graphQlSchema, "dgraph.graphql.schema", Some(""), None, None, None, None, None, None, "string"),
      TypedNode(st1, "dgraph.type", Some("Film"), None, None, None, None, None, None, "string"),
      TypedNode(st1, "name", Some("Star Trek: The Motion Picture"), None, None, None, None, None, None, "string"),
      TypedNode(st1, "release_date", None, None, None, Some(Timestamp.valueOf("1979-12-07 00:00:00.0")), None, None, None, "timestamp"),
      TypedNode(st1, "revenue", None, None, Some(1.39E8), None, None, None, None, "double"),
      TypedNode(st1, "running_time", None, Some(132), None, None, None, None, None, "long"),
      TypedNode(leia, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
      TypedNode(leia, "name", Some("Princess Leia"), None, None, None, None, None, None, "string"),
      TypedNode(lucas, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
      TypedNode(lucas, "name", Some("George Lucas"), None, None, None, None, None, None, "string"),
      TypedNode(irvin, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
      TypedNode(irvin, "name", Some("Irvin Kernshner"), None, None, None, None, None, None, "string"),
      TypedNode(sw1, "dgraph.type", Some("Film"), None, None, None, None, None, None, "string"),
      TypedNode(sw1, "name", Some("Star Wars: Episode IV - A New Hope"), None, None, None, None, None, None, "string"),
      TypedNode(sw1, "release_date", None, None, None, Some(Timestamp.valueOf("1977-05-25 00:00:00.0")), None, None, None, "timestamp"),
      TypedNode(sw1, "revenue", None, None, Some(7.75E8), None, None, None, None, "double"),
      TypedNode(sw1, "running_time", None, Some(121), None, None, None, None, None, "long"),
      TypedNode(sw2, "dgraph.type", Some("Film"), None, None, None, None, None, None, "string"),
      TypedNode(sw2, "name", Some("Star Wars: Episode V - The Empire Strikes Back"), None, None, None, None, None, None, "string"),
      TypedNode(sw2, "release_date", None, None, None, Some(Timestamp.valueOf("1980-05-21 00:00:00.0")), None, None, None, "timestamp"),
      TypedNode(sw2, "revenue", None, None, Some(5.34E8), None, None, None, None, "double"),
      TypedNode(sw2, "running_time", None, Some(124), None, None, None, None, None, "long"),
      TypedNode(luke, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
      TypedNode(luke, "name", Some("Luke Skywalker"), None, None, None, None, None, None, "string"),
      TypedNode(han, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
      TypedNode(han, "name", Some("Han Solo"), None, None, None, None, None, None, "string"),
      TypedNode(richard, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
      TypedNode(richard, "name", Some("Richard Marquand"), None, None, None, None, None, None, "string"),
      TypedNode(sw3, "dgraph.type", Some("Film"), None, None, None, None, None, None, "string"),
      TypedNode(sw3, "name", Some("Star Wars: Episode VI - Return of the Jedi"), None, None, None, None, None, None, "string"),
      TypedNode(sw3, "release_date", None, None, None, Some(Timestamp.valueOf("1983-05-25 00:00:00.0")), None, None, None, "timestamp"),
      TypedNode(sw3, "revenue", None, None, Some(5.72E8), None, None, None, None, "double"),
      TypedNode(sw3, "running_time", None, Some(131), None, None, None, None, None, "long"),
    )

    def doTestLoadTypedNodes(load: () => DataFrame): Unit = {
      val nodes = load().as[TypedNode].collect().toSet
      assert(nodes === expectedTypedNodes)
    }

    lazy val expectedWideNodes = Set(
      Row(graphQlSchema, "", "dgraph.graphql.schema", "dgraph.graphql", null, null, null, null),
      Row(st1, null, null, "Film", "Star Trek: The Motion Picture", Timestamp.valueOf("1979-12-07 00:00:00.0"), 1.39E8, 132),
      Row(leia, null, null, "Person", "Princess Leia", null, null, null),
      Row(lucas, null, null, "Person", "George Lucas", null, null, null),
      Row(irvin, null, null, "Person", "Irvin Kernshner", null, null, null),
      Row(sw1, null, null, "Film", "Star Wars: Episode IV - A New Hope", Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121),
      Row(sw2, null, null, "Film", "Star Wars: Episode V - The Empire Strikes Back", Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124),
      Row(luke, null, null, "Person", "Luke Skywalker", null, null, null),
      Row(han, null, null, "Person", "Han Solo", null, null, null),
      Row(richard, null, null, "Person", "Richard Marquand", null, null, null),
      Row(sw3, null, null, "Film", "Star Wars: Episode VI - Return of the Jedi", Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131)
    )

    def doTestLoadWideNodes(load: () => DataFrame): Unit = {
      val nodes = load().collect().toSet
      assert(nodes === expectedWideNodes)
    }

    it("should load nodes via path") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .format(NodesSource)
          .load(cluster.grpc)
      )
    }

    it("should load nodes via paths") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .format(NodesSource)
          .load(cluster.grpc, cluster.grpcLocalIp)
      )
    }

    it("should load nodes via target option") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .format(NodesSource)
          .option(TargetOption, cluster.grpc)
          .load()
      )
    }

    it("should load nodes via targets option") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .format(NodesSource)
          .option(TargetsOption, s"""["${cluster.grpc}","${cluster.grpcLocalIp}"]""")
          .load()
      )
    }

    it("should load nodes via implicit dgraph target") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .dgraphNodes(cluster.grpc)
      )
    }

    it("should load nodes via implicit dgraph targets") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .dgraphNodes(cluster.grpc, cluster.grpcLocalIp)
      )
    }

    it("should load typed nodes") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeTypedOption)
          .dgraphNodes(cluster.grpc)
      )
    }

    it("should load wide nodes") {
      doTestLoadWideNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeWideOption)
          .dgraphNodes(cluster.grpc)
      )
    }

    it("should load wide nodes with predicate partitioner") {
      doTestLoadWideNodes(() =>
        spark
          .read
          .options(Map(
            NodesModeOption -> NodesModeWideOption,
            PartitionerOption -> PredicatePartitionerOption,
            PredicatePartitionerPredicatesOption -> "1"
          ))
          .dgraphNodes(cluster.grpc)
      )
    }

    it("should load wide nodes with predicate partitioner and uid ranges") {
      doTestLoadWideNodes(() =>
        spark
          .read
          .options(Map(
            NodesModeOption -> NodesModeWideOption,
            PartitionerOption -> s"$PredicatePartitionerOption+$UidRangePartitionerOption",
            PredicatePartitionerPredicatesOption -> "1",
            UidRangePartitionerUidsPerPartOption -> "1",
            MaxLeaseIdEstimatorIdOption -> highestUid.toString
          ))
          .dgraphNodes(cluster.grpc)
      )
    }

    it("should load typed nodes in chunks") {
      // it is hard to test data are really read in chunks, but we can test the data are correct
      doTestLoadTypedNodes(() =>
        spark
          .read
          .options(Map(
            NodesModeOption -> NodesModeTypedOption,
            PartitionerOption -> PredicatePartitionerOption,
            PredicatePartitionerPredicatesOption -> "2",
            ChunkSizeOption -> "3"
          ))
          .dgraphNodes(cluster.grpc)
      )
    }

    it("should encode TypedNode") {
      val rows =
        spark
          .read
          .format(NodesSource)
          .load(cluster.grpc)
          .as[TypedNode]
          .collectAsList()
      assert(rows.size() === 35)
    }

    it("should fail without target") {
      assertThrows[IllegalArgumentException] {
        spark
          .read
          .format(NodesSource)
          .load()
      }
    }

    it("should fail with unknown mode") {
      assertThrows[IllegalArgumentException] {
        spark
          .read
          .format(NodesSource)
          .option(NodesModeOption, "unknown")
          .load()
      }
    }

    it("should load as a single partition") {
      val target = cluster.grpc
      val targets = Seq(Target(target))
      val partitions =
        spark
          .read
          .option(PartitionerOption, SingletonPartitionerOption)
          .dgraphNodes(target)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition => Some(p.inputPartition)
          case _ => None
        }
      val predicates = Set(
        Predicate("dgraph.graphql.schema", "string"),
        Predicate("dgraph.graphql.xid", "string"),
        Predicate("dgraph.type", "string"),
        Predicate("name", "string"),
        Predicate("release_date", "datetime"),
        Predicate("revenue", "float"),
        Predicate("running_time", "int")
      )
      assert(partitions === Seq(Some(Partition(targets).has(predicates))))
    }

    it("should load as a predicate partitions") {
      val target = cluster.grpc
      val partitions =
        spark
          .read
          .option(PartitionerOption, PredicatePartitionerOption)
          .option(PredicatePartitionerPredicatesOption, "2")
          .dgraphNodes(target)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition => Some(p.inputPartition)
          case _ => None
        }

      val expected = Set(
        Some(Partition(Seq(Target(cluster.grpc))).has(Set("dgraph.type"), Set.empty).getAll()),
        Some(Partition(Seq(Target(cluster.grpc))).has(Set("revenue"), Set.empty).getAll()),
        Some(Partition(Seq(Target(cluster.grpc))).has(Set("dgraph.graphql.schema", "dgraph.graphql.xid"), Set.empty).getAll()),
        Some(Partition(Seq(Target(cluster.grpc))).has(Set("running_time"), Set.empty).getAll()),
        Some(Partition(Seq(Target(cluster.grpc))).has(Set("release_date", "name"), Set.empty).getAll())
      )

      assert(partitions.toSet === expected)
    }

    it("should partition data") {
      val target = cluster.grpc
      val partitions =
        spark
          .read
          .options(Map(
            PartitionerOption -> UidRangePartitionerOption,
            UidRangePartitionerUidsPerPartOption -> "7",
            MaxLeaseIdEstimatorIdOption -> highestUid.toString
          ))
          .dgraphNodes(target)
          .mapPartitions(part => Iterator(part.map(_.getLong(0)).toSet))
          .collect()

      // ignore the existence or absence of graphQlSchema in the result, otherwise flaky test:
      // - should partition data *** FAILED ***
      //  Array(Set(5, 6, 2, 7, 3, 4), Set(10, 9, 12, 11, 8)) did not equal Stream(Set(5, 6, 2, 7, 3, 8, 4), Set(9, 10, 11, 12)) (TestNodeSource.scala:295)
      assert(partitions.map(_ - graphQlSchema) === allUids.grouped(7).map(_.toSet - graphQlSchema).toSeq)
    }

    lazy val typedNodes =
      spark
        .read
        .options(Map(
          NodesModeOption -> NodesModeTypedOption,
          PartitionerOption -> PredicatePartitionerOption,
          PredicatePartitionerPredicatesOption -> "2"
        ))
        .dgraphNodes(cluster.grpc)
        .as[TypedNode]

    lazy val wideNodes =
      spark
        .read
        .options(Map(
          NodesModeOption -> NodesModeWideOption,
          PartitionerOption -> PredicatePartitionerOption,
          PredicatePartitionerPredicatesOption -> "2"
        ))
        .dgraphNodes(cluster.grpc)

    it("should push predicate filters") {
      doTestFilterPushDown(typedNodes, $"predicate" === "name", Seq(IntersectPredicateNameIsIn("name")), expectedDs = expectedTypedNodes.filter(_.predicate == "name"))
      doTestFilterPushDown(typedNodes, $"predicate".isin("name"), Seq(IntersectPredicateNameIsIn("name")), expectedDs = expectedTypedNodes.filter(_.predicate == "name"))
      doTestFilterPushDown(typedNodes, $"predicate".isin("name", "starring"), Seq(IntersectPredicateNameIsIn("name", "starring")), expectedDs = expectedTypedNodes.filter(t => Set("name", "starring").contains(t.predicate)))
    }

    it("should push predicate value filters") {
      val columns = wideNodes.columns

      doTestFilterPushDown(wideNodes,
        $"name".isNotNull,
        Seq(PredicateNameIs("name")),
        expectedDs = expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("name"))).isDefined)
      )

      doTestFilterPushDown(wideNodes,
        $"name".isNotNull && $"running_time".isNotNull,
        Seq(PredicateNameIs("name"), PredicateNameIs("running_time")),
        expectedDs = expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("name"))).isDefined && !r.isNullAt(columns.indexOf("running_time")))
      )

      doTestFilterPushDown(wideNodes,
        $"name".isin("Star Wars: Episode IV - A New Hope"),
        Seq(SinglePredicateValueIsIn("name", Set("Star Wars: Episode IV - A New Hope"))),
        expectedDs = expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("name"))).exists(_.equals("Star Wars: Episode IV - A New Hope")))
      )

      doTestFilterPushDown(wideNodes,
        $"name".isin("Star Wars: Episode IV - A New Hope", "Princess Leia"),
        Seq(SinglePredicateValueIsIn("name", Set("Star Wars: Episode IV - A New Hope", "Princess Leia"))),
        expectedDs = expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("name"))).exists(Set("Star Wars: Episode IV - A New Hope", "Princess Leia").contains))
      )

      doTestFilterPushDown(wideNodes,
        $"name" === "Star Wars: Episode IV - A New Hope" && $"running_time" === 121,
        Seq(SinglePredicateValueIsIn("name", Set("Star Wars: Episode IV - A New Hope")), SinglePredicateValueIsIn("running_time", Set(121))),
        expectedDs = expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("name"))).exists(_.equals("Star Wars: Episode IV - A New Hope")) && Option(r.getInt(columns.indexOf("running_time"))).exists(_.equals(121)))
      )

      val expected = expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("name"))).exists(_.equals("Luke Skywalker")) && (if (r.isNullAt(columns.indexOf("running_time"))) None else Some(r.getInt(columns.indexOf("running_time")))).exists(_.equals(121)))
      assert(expected.isEmpty, "expect empty result for this query, check query")
      doTestFilterPushDown(wideNodes,
        $"name" === "Luke Skywalker" && $"running_time" === 121,
        Seq(SinglePredicateValueIsIn("name", Set("Luke Skywalker")), SinglePredicateValueIsIn("running_time", Set(121))),
        expectedDs = expected
      )
    }

    it("should push object type filters") {
      doTestFilterPushDown(typedNodes, $"objectType" === "string", Seq(ObjectTypeIsIn("string")), expectedDs = expectedTypedNodes.filter(_.objectType == "string"))
      doTestFilterPushDown(typedNodes, $"objectType".isin("string"), Seq(ObjectTypeIsIn("string")), expectedDs = expectedTypedNodes.filter(_.objectType == "string"))
      typedNodes.printSchema()
      typedNodes.where($"objectType".isin("string", "long")).show(100, false)
      doTestFilterPushDown(typedNodes, $"objectType".isin("string", "long"), Seq(ObjectTypeIsIn("string", "long")), expectedDs = expectedTypedNodes.filter(t => Set("string", "long").contains(t.objectType)))
    }

    it("should push object value filters") {
      doTestFilterPushDown[TypedNode](typedNodes,
        $"objectString".isNotNull,
        Seq(ObjectTypeIsIn("string")),
        expectedDs = expectedTypedNodes.filter(_.objectString.isDefined)
      )
      doTestFilterPushDown(typedNodes,
        $"objectString".isNotNull && $"objectLong".isNotNull,
        Seq(AlwaysFalse),
        expectedDs = Set.empty
      )

      doTestFilterPushDown(typedNodes,
        $"objectString" === "Person",
        Seq(ObjectValueIsIn("Person"), ObjectTypeIsIn("string")),
        expectedDs = expectedTypedNodes.filter(_.objectString.exists(_.equals("Person")))
      )

      doTestFilterPushDown(typedNodes,
        $"objectString".isin("Person"),
        Seq(ObjectValueIsIn("Person"), ObjectTypeIsIn("string")),
        expectedDs = expectedTypedNodes.filter(_.objectString.exists(_.equals("Person")))
      )

      doTestFilterPushDown(typedNodes,
        $"objectString".isin("Person", "Film"),
        Seq(ObjectValueIsIn("Person", "Film"), ObjectTypeIsIn("string")),
        expectedDs = expectedTypedNodes.filter(_.objectString.exists(s => Set("Person", "Film").contains(s)))
      )

      doTestFilterPushDown(typedNodes,
        $"objectString" === "Person" && $"objectLong" === 1,
        Seq(AlwaysFalse),
        expectedDs = Set.empty
      )
    }

    def doTestFilterPushDown[T](df: Dataset[T], condition: Column, expectedFilters: Seq[Filter], expectedUnpushed: Seq[Expression] = Seq.empty, expectedDs: Set[T]): Unit = {
      doTestFilterPushDownDf(df, condition, expectedFilters, expectedUnpushed, expectedDs)
    }

  }

}
