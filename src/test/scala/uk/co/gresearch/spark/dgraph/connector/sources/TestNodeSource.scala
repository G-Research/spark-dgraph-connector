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

import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.encoder.TypedNodeEncoder
import uk.co.gresearch.spark.dgraph.connector.model.NodeTableModel

class TestNodeSource extends FunSpec with SparkTestSession {

  import spark.implicits._

  describe("NodeDataSource") {

    it("should load nodes via path") {
      spark
        .read
        .format(NodesSource)
        .load("localhost:9080")
        .show(100, false)
    }

    it("should load nodes via paths") {
      spark
        .read
        .format(NodesSource)
        .load("localhost:9080", "127.0.0.1:9080")
        .show(100, false)
    }

    it("should load nodes via target option") {
      spark
        .read
        .format(NodesSource)
        .option(TargetOption, "localhost:9080")
        .load()
        .show(100, false)
    }

    it("should load nodes via targets option") {
      spark
        .read
        .format(NodesSource)
        .option(TargetsOption, "[\"localhost:9080\",\"127.0.0.1:9080\"]")
        .load()
        .show(100, false)
    }

    it("should load nodes via implicit dgraph target") {
      spark
        .read
        .dgraphNodes("localhost:9080")
        .show(100, false)
    }

    it("should load nodes via implicit dgraph targets") {
      spark
        .read
        .dgraphNodes("localhost:9080", "127.0.0.1:9080")
        .show(100, false)
    }

    it("should load typed nodes") {
      spark
        .read
        .option(NodesModeOption, NodesModeTypedOption)
        .dgraphNodes("localhost:9080")
        .show(100, false)
    }

    it("should load wide nodes") {
      spark
        .read
        .option(NodesModeOption, NodesModeWideOption)
        .dgraphNodes("localhost:9080")
        .show(100, false)
    }

    it("should encode TypedNode") {
      val rows =
        spark
          .read
          .format(NodesSource)
          .load("localhost:9080")
          .as[TypedNode]
          .collectAsList()
      rows.forEach(println)
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
          .dgraphNodes(target)
          .mapPartitions(part => Iterator(part.map(_.getLong(0)).toSet))
          .collect()
      assert(partitions.length === 10)
      assert(partitions === Seq((1 to 10).toSet) ++ (1 to 9).map(_ => Set.empty[Long]))
    }

  }

}
