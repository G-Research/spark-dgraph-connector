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

class TestNodeSource extends FunSpec
  with SparkTestSession with DgraphTestCluster {

  import spark.implicits._

  describe("NodeDataSource") {

    def doTestLoadTypedNodes(load: () => DataFrame): Unit = {
      val nodes = load().as[TypedNode].collect().toSet
      val expected = Set(
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
      assert(nodes === expected)
    }

    def doTestLoadWideNodes(load: () => DataFrame): Unit = {
      val nodes = load().collect().toSet
      val expected = Set(
        Row(st1, null, "Film", "Star Trek: The Motion Picture", Timestamp.valueOf("1979-12-07 00:00:00.0"), 1.39E8, 132),
        Row(leia, null, "Person", "Princess Leia", null, null, null),
        Row(lucas, null, "Person", "George Lucas", null, null, null),
        Row(irvin, null, "Person", "Irvin Kernshner", null, null, null),
        Row(sw1, null, "Film", "Star Wars: Episode IV - A New Hope", Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121),
        Row(sw2, null, "Film", "Star Wars: Episode V - The Empire Strikes Back", Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124),
        Row(luke, null, "Person", "Luke Skywalker", null, null, null),
        Row(han, null, "Person", "Han Solo", null, null, null),
        Row(richard, null, "Person", "Richard Marquand", null, null, null),
        Row(sw3, null, "Film", "Star Wars: Episode VI - Return of the Jedi", Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131)
      )
      assert(nodes === expected)
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
          case p: DataSourceRDDPartition => Some(p.inputPartition)
          case _ => None
        }
      assert(partitions.length === 1)
      assert(partitions === Seq(Some(Partition(targets, None, None))))
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
          case p: DataSourceRDDPartition => Some(p.inputPartition)
          case _ => None
        }
      assert(partitions.length === 4)
      assert(partitions === Seq(
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("release_date", "datetime"), Predicate("running_time", "int"))), None)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("dgraph.graphql.schema", "string"), Predicate("name", "string"))), None)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("dgraph.type", "string"))), None)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("revenue", "float"))), None))
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
