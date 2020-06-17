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

import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.encoder.{EdgeEncoder, TypedTripleEncoder}
import uk.co.gresearch.spark.dgraph.connector.model.{EdgeTableModel, TripleTableModel}

class TestEdgeSource extends FunSpec
  with SparkTestSession with DgraphTestCluster {

  import spark.implicits._

  describe("EdgeDataSource") {

    def doTestLoadEdges(load: () => DataFrame): Unit = {
      val columns = Encoders.product[Edge].schema.fields.map(_.name).map(col)
      val edges = load().coalesce(1).sortWithinPartitions(columns: _*)
      assert(edges.collect() === Seq(
        Row(5, "director", 3),
        Row(5, "starring", 2),
        Row(5, "starring", 7),
        Row(5, "starring", 8),
        Row(6, "director", 4),
        Row(6, "starring", 2),
        Row(6, "starring", 7),
        Row(6, "starring", 8),
        Row(10, "director", 9),
        Row(10, "starring", 2),
        Row(10, "starring", 7),
        Row(10, "starring", 8),
      ))
    }

    it("should load edges via path") {
      doTestLoadEdges( () =>
        spark
          .read
          .format(EdgesSource)
          .load("localhost:9080")
      )
    }

    it("should load edges via paths") {
      doTestLoadEdges(() =>
        spark
          .read
          .format(EdgesSource)
          .load("localhost:9080", "127.0.0.1:9080")
      )
    }

    it("should load edges via target option") {
      doTestLoadEdges(() =>
        spark
          .read
          .format(EdgesSource)
          .option(TargetOption, "localhost:9080")
          .load()
      )
    }

    it("should load edges via targets option") {
      doTestLoadEdges(() =>
        spark
          .read
          .format(EdgesSource)
          .option(TargetsOption, "[\"localhost:9080\",\"127.0.0.1:9080\"]")
          .load()
      )
    }

    it("should load edges via implicit dgraph target") {
      doTestLoadEdges( () =>
      spark
        .read
        .dgraphEdges("localhost:9080")
      )
    }

    it("should load edges via implicit dgraph targets") {
      doTestLoadEdges(() =>
        spark
          .read
          .dgraphEdges("localhost:9080", "127.0.0.1:9080")
      )
    }

    it("should encode Edge") {
      val rows =
        spark
          .read
          .format(EdgesSource)
          .load("localhost:9080")
          .as[Edge]
          .collectAsList()
      assert(rows.size() === 12)
    }

    it("should fail without target") {
      assertThrows[IllegalArgumentException] {
        spark
          .read
          .format(EdgesSource)
          .load()
      }
    }

    val schema = Schema(Set(
      Predicate("director", "uid"),
      Predicate("starring", "uid")
    ))
    val encoder = EdgeEncoder(schema.predicateMap)
    val model = EdgeTableModel(encoder)

    it("should load as a single partition") {
      val target = "localhost:9080"
      val targets = Seq(Target(target))
      val partitions =
        spark
          .read
          .option(PartitionerOption, SingletonPartitionerOption)
          .dgraphEdges(target)
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
          .dgraphEdges(target)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition[_] => Some(p.inputPartition)
          case _ => None
        }
      assert(partitions.length === 2)
      assert(partitions === Seq(
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("director", "uid"))), None, model)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("starring", "uid"))), None, model))
      ))
    }

    it("should partition data") {
      val target = "localhost:9080"
      val partitions =
        spark
          .read
          .options(Map(
            UidRangePartitionerUidsPerPartOption -> "2",
            UidRangePartitionerEstimatorOption -> UidCountEstimatorOption,
          ))
          .dgraphEdges(target)
          .mapPartitions(part => Iterator(part.map(_.getLong(0)).toSet))
          .collect()
      assert(partitions.length === 5)
      // we can only count and retrieve triples, not edges only, and filter for edges in the connector
      // this produces empty partitions: https://github.com/G-Research/spark-dgraph-connector/issues/19
      assert(partitions.map(_.size) === Seq(0, 0, 2, 0, 1))
    }

  }

}
