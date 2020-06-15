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

class TestEdgeSource extends FunSpec with SparkTestSession {

  import spark.implicits._

  describe("EdgeDataSource") {

    it("should load edges via path") {
      spark
        .read
        .format(EdgesSource)
        .load("localhost:9080")
        .printSchema()
    }

    it("should load edges via paths") {
      spark
        .read
        .format(EdgesSource)
        .load("localhost:9080", "127.0.0.1:9080")
        .show(100, false)
    }

    it("should load edges via target option") {
      spark
        .read
        .format(EdgesSource)
        .option(TargetOption, "localhost:9080")
        .load()
        .show(100, false)
    }

    it("should load edges via targets option") {
      spark
        .read
        .format(EdgesSource)
        .option(TargetsOption, "[\"localhost:9080\",\"127.0.0.1:9080\"]")
        .load()
        .show(100, false)
    }

    it("should load edges via implicit dgraph target") {
      spark
        .read
        .dgraphEdges("localhost:9080")
        .show(100, false)
    }

    it("should load edges via implicit dgraph targets") {
      spark
        .read
        .dgraphEdges("localhost:9080", "127.0.0.1:9080")
        .show(100, false)
    }

    it("should encode Edge") {
      val rows =
        spark
          .read
          .format(EdgesSource)
          .load("localhost:9080")
          .as[Edge]
          .collectAsList()
      rows.forEach(println)
    }

    it("should fail without target") {
      assertThrows[IllegalArgumentException] {
        spark
          .read
          .format(EdgesSource)
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
          .dgraphEdges(target)
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
          .dgraphEdges(target)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition => Some(p.inputPartition)
          case _ => None
        }
      assert(partitions.length === 2)
      assert(partitions === Seq(
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("director", "uid"))), None)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("starring", "uid"))), None))
      ))
    }

    it("should partition data") {
      val target = "localhost:9080"
      val partitions =
        spark
          .read
          .dgraphEdges(target)
          .mapPartitions(part => Iterator(part.map(_.getLong(0)).toSet))
          .collect()
      assert(partitions.length === 10)
      assert(partitions.map(_.size) === Seq(3) ++ (1 to 9).map(_ => 0))
    }

  }

}
