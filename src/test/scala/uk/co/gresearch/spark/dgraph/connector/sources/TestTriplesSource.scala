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
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.encoder.TypedTripleEncoder
import uk.co.gresearch.spark.dgraph.connector.model.TripleTableModel

class TestTriplesSource extends FunSpec
  with SparkTestSession with DgraphTestCluster {

  import spark.implicits._

  describe("TriplesDataSource") {

    it("should load triples via path") {
      spark
        .read
        .format(TriplesSource)
        .load("localhost:9080")
        .show(100, false)
    }

    it("should load triples via paths") {
      spark
        .read
        .format(TriplesSource)
        .load("localhost:9080", "127.0.0.1:9080")
        .show(100, false)
    }

    it("should load triples via target option") {
      spark
        .read
        .format(TriplesSource)
        .option(TargetOption, "localhost:9080")
        .load()
        .show(100, false)
    }

    it("should load triples via targets option") {
      spark
        .read
        .format(TriplesSource)
        .option(TargetsOption, "[\"localhost:9080\",\"127.0.0.1:9080\"]")
        .load()
        .show(100, false)
    }

    it("should load triples via implicit dgraph target") {
      spark
        .read
        .dgraphTriples("localhost:9080")
        .show(100, false)
    }

    it("should load triples via implicit dgraph targets") {
      spark
        .read
        .dgraphTriples("localhost:9080", "127.0.0.1:9080")
        .show(100, false)
    }

    it("should load string-object triples") {
      spark
        .read
        .option(TriplesModeOption, TriplesModeStringOption)
        .dgraphTriples("localhost:9080")
        .show(100, false)
    }

    it("should load typed-object triples") {
      spark
        .read
        .option(TriplesModeOption, TriplesModeTypedOption)
        .dgraphTriples("localhost:9080")
        .show(100, false)
    }

    it("should encode StringTriple") {
      val rows =
        spark
          .read
          .option(TriplesModeOption, TriplesModeStringOption)
          .dgraphTriples("localhost:9080")
          .as[StringTriple]
          .collectAsList()
      rows.forEach(println)
      assert(rows.size() == 44)
    }

    it("should encode TypedTriple") {
      val rows =
        spark
          .read
          .option(TriplesModeOption, TriplesModeTypedOption)
          .dgraphTriples("localhost:9080")
          .as[TypedTriple]
          .collectAsList()
      rows.forEach(println)
      assert(rows.size() == 44)
    }

    it("should fail without target") {
      assertThrows[IllegalArgumentException] {
        spark
          .read
          .format(TriplesSource)
          .load()
      }
    }

    it("should fail with unknown triple mode") {
      assertThrows[IllegalArgumentException] {
        spark
          .read
          .format(TriplesSource)
          .option(TriplesModeOption, "unknown")
          .load()
      }
    }

    val schema = Schema(Set(
      Predicate("release_date", "datetime"),
      Predicate("revenue", "float"),
      Predicate("running_time", "int"),
      Predicate("name", "string"),
      Predicate("dgraph.graphql.schema", "string"),
      Predicate("dgraph.type", "string"),
      Predicate("director", "uid"),
      Predicate("starring", "uid")
    ))
    val encoder = TypedTripleEncoder(schema.predicateMap)
    val model = TripleTableModel(encoder)

    it("should load as a single partition") {
      val target = "localhost:9080"
      val targets = Seq(Target(target))
      val partitions =
        spark
          .read
          .option(PartitionerOption, SingletonPartitionerOption)
          .dgraphTriples(target)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition[_] => Some(p.inputPartition)
          case _ => None
        }
      assert(partitions.length === 1)
      assert(partitions === Seq(Some(Partition(targets, None, None, model))))
    }

    it("should load as predicate partitions") {
      val target = "localhost:9080"
      val partitions =
        spark
          .read
          .option(PartitionerOption, PredicatePartitionerOption)
          .option(PredicatePartitionerPredicatesOption, "2")
          .dgraphTriples(target)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition[_] => Some(p.inputPartition)
          case _ => None
        }
      assert(partitions.length === 4)
      assert(partitions === Seq(
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("release_date", "datetime"), Predicate("revenue", "float"))), None, model)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("dgraph.graphql.schema", "string"), Predicate("starring", "uid"))), None, model)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("director", "uid"), Predicate("running_time", "int"))), None, model)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("dgraph.type", "string"), Predicate("name", "string"))), None, model))
      ))
    }

    it("should load as uid-range partitions") {
      val target = "localhost:9080"
      val partitions =
        spark
          .read
          .option(PartitionerOption, s"$UidRangePartitionerOption")
          .option(UidRangePartitionerUidsPerPartOption, "5000")
          .dgraphTriples(target)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition[_] => Some(p.inputPartition)
          case _ => None
        }

      val expected = (0 to 1).map(idx =>
        Some(Partition(Seq(Target("localhost:9080")), None, Some(UidRange(idx * 5000, 5000)), model))
      )

      assert(partitions.length === expected.size)
      assert(partitions === expected)
    }

    it("should load as predicate uid-range partitions") {
      val target = "localhost:9080"
      val partitions =
        spark
          .read
          .option(PartitionerOption, s"$PredicatePartitionerOption+$UidRangePartitionerOption")
          .option(PredicatePartitionerPredicatesOption, "2")
          .option(UidRangePartitionerUidsPerPartOption, "5000")
          .dgraphTriples(target)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition[_] => Some(p.inputPartition)
          case _ => None
        }

      val expected = Seq(0, 5000).flatMap( first =>
        Seq(
          Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("release_date", "datetime"), Predicate("revenue", "float"))), Some(UidRange(first, 5000)), model)),
          Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("dgraph.graphql.schema", "string"), Predicate("starring", "uid"))), Some(UidRange(first, 5000)), model)),
          Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("director", "uid"), Predicate("running_time", "int"))), Some(UidRange(first, 5000)), model)),
          Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("dgraph.type", "string"), Predicate("name", "string"))), Some(UidRange(first, 5000)), model))
        )
      )

      assert(partitions.length === expected.size)
      assert(partitions === expected)
    }

    it("should partition data") {
      val target = "localhost:9080"
      val partitions =
        spark
          .read
          .dgraphTriples(target)
          .mapPartitions(part => Iterator(part.map(_.getLong(0)).toSet))
          .collect()
      assert(partitions.length === 10)
      assert(partitions === Seq((1 to 10).toSet) ++ (1 to 9).map(_ => Set.empty[Long]))
    }

  }

}
