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

import org.apache.spark.scheduler.{AccumulableInfo, SparkListener}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.encoder.TypedNodeEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.DgraphExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.model.NodeTableModel
import uk.co.gresearch.spark.{SparkEventCollector, SparkTestSession}

import scala.collection.mutable

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

    val schema = Schema(Set(
      Predicate("release_date", "datetime"),
      Predicate("revenue", "float"),
      Predicate("running_time", "int"),
      Predicate("name", "string"),
      Predicate("dgraph.graphql.schema", "string"),
      Predicate("dgraph.graphql.xid", "string"),
      Predicate("dgraph.type", "string")
    ))
    val execution = DgraphExecutorProvider()
    val encoder = TypedNodeEncoder(schema.predicateMap)
    val model = NodeTableModel(execution, encoder, ChunkSizeDefault)

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
          case p: DataSourceRDDPartition[_] => Some(p.inputPartition)
          case _ => None
        }
      assert(partitions === Seq(Some(Partition(targets, None, None, None, model))))
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
          case p: DataSourceRDDPartition[_] => Some(p.inputPartition)
          case _ => None
        }

      val expected = Set(
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("dgraph.type", "string"))), None, None, model)),
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("revenue", "float"))), None, None, model)),
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("dgraph.graphql.schema", "string"), Predicate("dgraph.graphql.xid", "string"))), None, None, model)),
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("running_time", "int"))), None, None, model)),
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("release_date", "datetime"), Predicate("name", "string"))), None, None, model))
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
      doTestFilterPushDown(typedNodes, $"predicate" === "name", Seq(PredicateNameIsIn("name")), expectedDs = expectedTypedNodes.filter(_.predicate == "name"))
      doTestFilterPushDown(typedNodes, $"predicate".isin("name"), Seq(PredicateNameIsIn("name")), expectedDs = expectedTypedNodes.filter(_.predicate == "name"))
      doTestFilterPushDown(typedNodes, $"predicate".isin("name", "starring"), Seq(PredicateNameIsIn("name", "starring")), expectedDs = expectedTypedNodes.filter(t => Set("name", "starring").contains(t.predicate)))
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

/**
    it("should push predicate value filters") {
      val columns = wideNodes.columns

      doTestFilterPushDown(wideNodes,
        $"name" === "Luke Skywalker",
        Seq(PredicateNameIsIn("name"), ObjectValueIsIn("Luke Skywalker")),
        Seq(IsNotNull(AttributeReference("name", StringType, nullable = true)())),
        expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("name"))).exists(_.equals("Luke Skywalker")))
      )

      doTestFilterPushDown(wideNodes,
        $"name".isin("Luke Skywalker"),
        Seq(PredicateNameIsIn("name"), ObjectValueIsIn("Luke Skywalker")),
        Seq(IsNotNull(AttributeReference("name", StringType, nullable = true)())),
        expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("name"))).exists(_.equals("Luke Skywalker")))
      )

      doTestFilterPushDown(wideNodes,
        $"name".isin("Luke Skywalker", "Princess Leia"),
        Seq(PredicateNameIsIn("name"), ObjectValueIsIn("Luke Skywalker", "Princess Leia")),
        Seq.empty,
        expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("name"))).exists(Set("Luke Skywalker", "Princess Leia").contains))
      )

      doTestFilterPushDown(wideNodes,
        $"name" === "Luke Skywalker" && $"running_time" === 121,
        Seq(AlwaysFalse),
        Seq.empty,
        expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("name"))).exists(_.equals("Luke Skywalker")) && Option(r.getInt(r.fieldIndex("running_time"))).exists(_.equals(121)))
      )
    }
*/

    def doTestFilterPushDown[T](df: Dataset[T], condition: Column, expectedFilters: Seq[Filter], expectedUnpushed: Seq[Expression] = Seq.empty, expectedDs: Set[T]): Unit = {
      doTestFilterPushDownDf(df, condition, expectedFilters, expectedUnpushed, expectedDs)
    }

    it("should provide metrics") {
      val accus: mutable.MutableList[AccumulableInfo] = mutable.MutableList.empty[AccumulableInfo]
      val handler: SparkListener = SparkEventCollector(accus)

      val df =
        spark
          .read
          .format(NodesSource)
          .load(cluster.grpc)

      spark.sparkContext.addSparkListener(handler)
      df.count()
      spark.sparkContext.removeSparkListener(handler)

      val accums =
        accus
          .filter(_.name.isDefined)
          .map(acc => acc.name.get -> acc.value)
          .toMap
          .filterKeys(_.startsWith("Dgraph"))

      assert(accums.keySet == Set("Dgraph Bytes", "Dgraph Uids", "Dgraph Chunks", "Dgraph Time"))
      assert(accums.get("Dgraph Bytes").flatten === Some(1178))
      assert(accums.get("Dgraph Uids").flatten === Some(11))
      assert(accums.get("Dgraph Chunks").flatten === Some(1))
      assert(accums.get("Dgraph Time").flatten.isDefined)
      assert(accums.get("Dgraph Time").flatten.get.isInstanceOf[Double])
    }

  }

}
