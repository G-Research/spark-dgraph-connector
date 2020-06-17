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

import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.encoder.TypedNodeEncoder
import uk.co.gresearch.spark.dgraph.connector.model.NodeTableModel

class TestNodeSource extends FunSpec
  with SparkTestSession with DgraphTestCluster {

  import spark.implicits._

  describe("NodeDataSource") {

    def doTestLoadTypedNodes(load: () => DataFrame): Unit = {
      val columns = Encoders.product[TypedNode].schema.fields.map(_.name).map(col)
      val nodes = load().as[TypedNode]
        .coalesce(1)
        .sortWithinPartitions(columns: _*)
      assert(nodes.collect() === Seq(
        TypedNode(1, "dgraph.type", Some("Film"), None, None, None, None, None, None, "string"),
        TypedNode(1, "name", Some("Star Trek: The Motion Picture"), None, None, None, None, None, None, "string"),
        TypedNode(1, "release_date", None, None, None, Some(Timestamp.valueOf("1979-12-07 00:00:00.0")), None, None, None, "timestamp"),
        TypedNode(1, "revenue", None, None, Some(1.39E8), None, None, None, None, "double"),
        TypedNode(1, "running_time", None, Some(132), None, None, None, None, None, "long"),
        TypedNode(2, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
        TypedNode(2, "name", Some("Princess Leia"), None, None, None, None, None, None, "string"),
        TypedNode(3, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
        TypedNode(3, "name", Some("George Lucas"), None, None, None, None, None, None, "string"),
        TypedNode(4, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
        TypedNode(4, "name", Some("Irvin Kernshner"), None, None, None, None, None, None, "string"),
        TypedNode(5, "dgraph.type", Some("Film"), None, None, None, None, None, None, "string"),
        TypedNode(5, "name", Some("Star Wars: Episode IV - A New Hope"), None, None, None, None, None, None, "string"),
        TypedNode(5, "release_date", None, None, None, Some(Timestamp.valueOf("1977-05-25 00:00:00.0")), None, None, None, "timestamp"),
        TypedNode(5, "revenue", None, None, Some(7.75E8), None, None, None, None, "double"),
        TypedNode(5, "running_time", None, Some(121), None, None, None, None, None, "long"),
        TypedNode(6, "dgraph.type", Some("Film"), None, None, None, None, None, None, "string"),
        TypedNode(6, "name", Some("Star Wars: Episode V - The Empire Strikes Back"), None, None, None, None, None, None, "string"),
        TypedNode(6, "release_date", None, None, None, Some(Timestamp.valueOf("1980-05-21 00:00:00.0")), None, None, None, "timestamp"),
        TypedNode(6, "revenue", None, None, Some(5.34E8), None, None, None, None, "double"),
        TypedNode(6, "running_time", None, Some(124), None, None, None, None, None, "long"),
        TypedNode(7, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
        TypedNode(7, "name", Some("Luke Skywalker"), None, None, None, None, None, None, "string"),
        TypedNode(8, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
        TypedNode(8, "name", Some("Han Solo"), None, None, None, None, None, None, "string"),
        TypedNode(9, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
        TypedNode(9, "name", Some("Richard Marquand"), None, None, None, None, None, None, "string"),
        TypedNode(10, "dgraph.type", Some("Film"), None, None, None, None, None, None, "string"),
        TypedNode(10, "name", Some("Star Wars: Episode VI - Return of the Jedi"), None, None, None, None, None, None, "string"),
        TypedNode(10, "release_date", None, None, None, Some(Timestamp.valueOf("1983-05-25 00:00:00.0")), None, None, None, "timestamp"),
        TypedNode(10, "revenue", None, None, Some(5.72E8), None, None, None, None, "double"),
        TypedNode(10, "running_time", None, Some(131), None, None, None, None, None, "long"),
      ))
    }

    def doTestLoadWideNodes(load: () => DataFrame): Unit = {
      val nodes = load().coalesce(1)
      val sorted = nodes.sortWithinPartitions(nodes.columns.map(c => col(s"`${c}`")): _*)
      assert(sorted.collect() === Seq(
        Row(1, null, "Film", "Star Trek: The Motion Picture", Timestamp.valueOf("1979-12-07 00:00:00.0"), 1.39E8, 132),
        Row(2, null, "Person", "Princess Leia", null, null, null),
        Row(3, null, "Person", "George Lucas", null, null, null),
        Row(4, null, "Person", "Irvin Kernshner", null, null, null),
        Row(5, null, "Film", "Star Wars: Episode IV - A New Hope", Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121),
        Row(6, null, "Film", "Star Wars: Episode V - The Empire Strikes Back", Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124),
        Row(7, null, "Person", "Luke Skywalker", null, null, null),
        Row(8, null, "Person", "Han Solo", null, null, null),
        Row(9, null, "Person", "Richard Marquand", null, null, null),
        Row(10, null, "Film", "Star Wars: Episode VI - Return of the Jedi", Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131)
      ))
    }

    it("should load nodes via path") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .format(NodesSource)
          .load("localhost:9080")
      )
    }

    it("should load nodes via paths") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .format(NodesSource)
          .load("localhost:9080", "127.0.0.1:9080")
      )
    }

    it("should load nodes via target option") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .format(NodesSource)
          .option(TargetOption, "localhost:9080")
          .load()
      )
    }

    it("should load nodes via targets option") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .format(NodesSource)
          .option(TargetsOption, "[\"localhost:9080\",\"127.0.0.1:9080\"]")
          .load()
      )
    }

    it("should load nodes via implicit dgraph target") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .dgraphNodes("localhost:9080")
      )
    }

    it("should load nodes via implicit dgraph targets") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .dgraphNodes("localhost:9080", "127.0.0.1:9080")
      )
    }

    it("should load typed nodes") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeTypedOption)
          .dgraphNodes("localhost:9080")
      )
    }

    it("should load wide nodes") {
      doTestLoadWideNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeWideOption)
          .dgraphNodes("localhost:9080")
      )
    }

    it("should encode TypedNode") {
      val rows =
        spark
          .read
          .format(NodesSource)
          .load("localhost:9080")
          .as[TypedNode]
          .collectAsList()
      assert(rows.size() === 32)
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
      Predicate("dgraph.type", "string")
    ))
    val encoder = TypedNodeEncoder(schema.predicateMap)
    val model = NodeTableModel(encoder)

    it("should load as a single partition") {
      val target = "localhost:9080"
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
      assert(partitions.length === 1)
      assert(partitions === Seq(Some(Partition(targets, None, None, model))))
    }

    it("should load as a predicate partitions") {
      val target = "localhost:9080"
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
      assert(partitions.length === 4)
      assert(partitions === Seq(
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("release_date", "datetime"), Predicate("running_time", "int"))), None, model)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("dgraph.graphql.schema", "string"), Predicate("name", "string"))), None, model)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("dgraph.type", "string"))), None, model)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("revenue", "float"))), None, model))
      ))
    }

    it("should partition data") {
      val target = "localhost:9080"
      val partitions =
        spark
          .read
          .option(UidRangePartitionerUidsPerPartOption, "7")
          .dgraphNodes(target)
          .mapPartitions(part => Iterator(part.map(_.getLong(0)).toSet))
          .collect()
      assert(partitions.length === 2)
      assert(partitions === Seq((1 to 7).toSet, (8 to 10).toSet))
    }

  }

}
