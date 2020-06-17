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

import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Encoders}
import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector._

class TestTriplesSource extends FunSpec
  with SparkTestSession with DgraphTestCluster {

  import spark.implicits._

  describe("TriplesDataSource") {

    def doTestLoadTypedTriples(load: () => DataFrame): Unit = {
      val columns = Encoders.product[TypedTriple].schema.fields.map(_.name).map(col)
      val triples = load().coalesce(1).sortWithinPartitions(columns: _*)
      assert(triples.as[TypedTriple].collect() === Seq(
        TypedTriple(1, "dgraph.type", None, Some("Film"), None, None, None, None, None, None, "string"),
        TypedTriple(1, "name", None, Some("Star Trek: The Motion Picture"), None, None, None, None, None, None, "string"),
        TypedTriple(1, "release_date", None, None, None, None, Some(Timestamp.valueOf("1979-12-07 00:00:00.0")), None, None, None, "timestamp"),
        TypedTriple(1, "revenue", None, None, None, Some(1.39E8), None, None, None, None, "double"),
        TypedTriple(1, "running_time", None, None, Some(132), None, None, None, None, None, "long"),
        TypedTriple(2, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
        TypedTriple(2, "name", None, Some("Princess Leia"), None, None, None, None, None, None, "string"),
        TypedTriple(3, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
        TypedTriple(3, "name", None, Some("George Lucas"), None, None, None, None, None, None, "string"),
        TypedTriple(4, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
        TypedTriple(4, "name", None, Some("Irvin Kernshner"), None, None, None, None, None, None, "string"),
        TypedTriple(5, "dgraph.type", None, Some("Film"), None, None, None, None, None, None, "string"),
        TypedTriple(5, "director", Some(3), None, None, None, None, None, None, None, "uid"),
        TypedTriple(5, "name", None, Some("Star Wars: Episode IV - A New Hope"), None, None, None, None, None, None, "string"),
        TypedTriple(5, "release_date", None, None, None, None, Some(Timestamp.valueOf("1977-05-25 00:00:00.0")), None, None, None, "timestamp"),
        TypedTriple(5, "revenue", None, None, None, Some(7.75E8), None, None, None, None, "double"),
        TypedTriple(5, "running_time", None, None, Some(121), None, None, None, None, None, "long"),
        TypedTriple(5, "starring", Some(2), None, None, None, None, None, None, None, "uid"),
        TypedTriple(5, "starring", Some(7), None, None, None, None, None, None, None, "uid"),
        TypedTriple(5, "starring", Some(8), None, None, None, None, None, None, None, "uid"),
        TypedTriple(6, "dgraph.type", None, Some("Film"), None, None, None, None, None, None, "string"),
        TypedTriple(6, "director", Some(4), None, None, None, None, None, None, None, "uid"),
        TypedTriple(6, "name", None, Some("Star Wars: Episode V - The Empire Strikes Back"), None, None, None, None, None, None, "string"),
        TypedTriple(6, "release_date", None, None, None, None, Some(Timestamp.valueOf("1980-05-21 00:00:00.0")), None, None, None, "timestamp"),
        TypedTriple(6, "revenue", None, None, None, Some(5.34E8), None, None, None, None, "double"),
        TypedTriple(6, "running_time", None, None, Some(124), None, None, None, None, None, "long"),
        TypedTriple(6, "starring", Some(2), None, None, None, None, None, None, None, "uid"),
        TypedTriple(6, "starring", Some(7), None, None, None, None, None, None, None, "uid"),
        TypedTriple(6, "starring", Some(8), None, None, None, None, None, None, None, "uid"),
        TypedTriple(7, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
        TypedTriple(7, "name", None, Some("Luke Skywalker"), None, None, None, None, None, None, "string"),
        TypedTriple(8, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
        TypedTriple(8, "name", None, Some("Han Solo"), None, None, None, None, None, None, "string"),
        TypedTriple(9, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
        TypedTriple(9, "name", None, Some("Richard Marquand"), None, None, None, None, None, None, "string"),
        TypedTriple(10, "dgraph.type", None, Some("Film"), None, None, None, None, None, None, "string"),
        TypedTriple(10, "director", Some(9), None, None, None, None, None, None, None, "uid"),
        TypedTriple(10, "name", None, Some("Star Wars: Episode VI - Return of the Jedi"), None, None, None, None, None, None, "string"),
        TypedTriple(10, "release_date", None, None, None, None, Some(Timestamp.valueOf("1983-05-25 00:00:00.0")), None, None, None, "timestamp"),
        TypedTriple(10, "revenue", None, None, None, Some(5.72E8), None, None, None, None, "double"),
        TypedTriple(10, "running_time", None, None, Some(131), None, None, None, None, None, "long"),
        TypedTriple(10, "starring", Some(2), None, None, None, None, None, None, None, "uid"),
        TypedTriple(10, "starring", Some(7), None, None, None, None, None, None, None, "uid"),
        TypedTriple(10, "starring", Some(8), None, None, None, None, None, None, None, "uid"),
      ))
    }

    def doTestLoadStringTriples(load: () => DataFrame): Unit = {
      val columns = Encoders.product[StringTriple].schema.fields.map(_.name).map(col)
      val triples = load().coalesce(1).sortWithinPartitions(columns: _*)
      assert(triples.as[StringTriple].collect() === Seq(
        StringTriple(1, "dgraph.type", "Film", "string"),
        StringTriple(1, "name", "Star Trek: The Motion Picture", "string"),
        StringTriple(1, "release_date", "1979-12-07 00:00:00.0", "timestamp"),
        StringTriple(1, "revenue", "1.39E8", "double"),
        StringTriple(1, "running_time", "132", "long"),
        StringTriple(2, "dgraph.type", "Person", "string"),
        StringTriple(2, "name", "Princess Leia", "string"),
        StringTriple(3, "dgraph.type", "Person", "string"),
        StringTriple(3, "name", "George Lucas", "string"),
        StringTriple(4, "dgraph.type", "Person", "string"),
        StringTriple(4, "name", "Irvin Kernshner", "string"),
        StringTriple(5, "dgraph.type", "Film", "string"),
        StringTriple(5, "director", "3", "uid"),
        StringTriple(5, "name", "Star Wars: Episode IV - A New Hope", "string"),
        StringTriple(5, "release_date", "1977-05-25 00:00:00.0", "timestamp"),
        StringTriple(5, "revenue", "7.75E8", "double"),
        StringTriple(5, "running_time", "121", "long"),
        StringTriple(5, "starring", "2", "uid"),
        StringTriple(5, "starring", "7", "uid"),
        StringTriple(5, "starring", "8", "uid"),
        StringTriple(6, "dgraph.type", "Film", "string"),
        StringTriple(6, "director", "4", "uid"),
        StringTriple(6, "name", "Star Wars: Episode V - The Empire Strikes Back", "string"),
        StringTriple(6, "release_date", "1980-05-21 00:00:00.0", "timestamp"),
        StringTriple(6, "revenue", "5.34E8", "double"),
        StringTriple(6, "running_time", "124", "long"),
        StringTriple(6, "starring", "2", "uid"),
        StringTriple(6, "starring", "7", "uid"),
        StringTriple(6, "starring", "8", "uid"),
        StringTriple(7, "dgraph.type", "Person", "string"),
        StringTriple(7, "name", "Luke Skywalker", "string"),
        StringTriple(8, "dgraph.type", "Person", "string"),
        StringTriple(8, "name", "Han Solo", "string"),
        StringTriple(9, "dgraph.type", "Person", "string"),
        StringTriple(9, "name", "Richard Marquand", "string"),
        StringTriple(10, "dgraph.type", "Film", "string"),
        StringTriple(10, "director", "9", "uid"),
        StringTriple(10, "name", "Star Wars: Episode VI - Return of the Jedi", "string"),
        StringTriple(10, "release_date", "1983-05-25 00:00:00.0", "timestamp"),
        StringTriple(10, "revenue", "5.72E8", "double"),
        StringTriple(10, "running_time", "131", "long"),
        StringTriple(10, "starring", "2", "uid"),
        StringTriple(10, "starring", "7", "uid"),
        StringTriple(10, "starring", "8", "uid"),
      ))
    }

    it("should load triples via path") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .format(TriplesSource)
          .load("localhost:9080")
      )
    }

    it("should load triples via paths") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .format(TriplesSource)
          .load("localhost:9080", "127.0.0.1:9080")
      )
    }

    it("should load triples via target option") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .format(TriplesSource)
          .option(TargetOption, "localhost:9080")
          .load()
      )
    }

    it("should load triples via targets option") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .format(TriplesSource)
          .option(TargetsOption, "[\"localhost:9080\",\"127.0.0.1:9080\"]")
          .load()
      )
    }

    it("should load triples via implicit dgraph target") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .dgraphTriples("localhost:9080")
      )
    }

    it("should load triples via implicit dgraph targets") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .dgraphTriples("localhost:9080", "127.0.0.1:9080")
      )
    }

    it("should load string-object triples") {
      doTestLoadStringTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeStringOption)
          .dgraphTriples("localhost:9080")
      )
    }

    it("should load typed-object triples") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeTypedOption)
          .dgraphTriples("localhost:9080")
      )
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
          case p: DataSourceRDDPartition => Some(p.inputPartition)
          case _ => None
        }
      assert(partitions.length === 1)
      assert(partitions === Seq(Some(Partition(targets, None, None))))
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
          case p: DataSourceRDDPartition => Some(p.inputPartition)
          case _ => None
        }
      assert(partitions.length === 4)
      assert(partitions === Seq(
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("release_date", "datetime"), Predicate("revenue", "float"))), None)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("dgraph.graphql.schema", "string"), Predicate("starring", "uid"))), None)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("director", "uid"), Predicate("running_time", "int"))), None)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("dgraph.type", "string"), Predicate("name", "string"))), None))
      ))
    }

    it("should load as uid-range partitions") {
      val target = "localhost:9080"
      val partitions =
        spark
          .read
          .option(PartitionerOption, s"$UidRangePartitionerOption")
          .option(UidRangePartitionerUidsPerPartOption, "7")
          .dgraphTriples(target)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition => Some(p.inputPartition)
          case _ => None
        }

      val expected = Seq(
        Some(Partition(Seq(Target("localhost:9080")), None, Some(UidRange(0, 7)))),
        Some(Partition(Seq(Target("localhost:9080")), None, Some(UidRange(7, 7)))),
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
          .option(UidRangePartitionerUidsPerPartOption, "5")
          .dgraphTriples(target)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition => Some(p.inputPartition)
          case _ => None
        }

      val expected = Seq(
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("release_date", "datetime"), Predicate("revenue", "float"))), None)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("dgraph.graphql.schema", "string"), Predicate("starring", "uid"))), None)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("director", "uid"), Predicate("running_time", "int"))), None)),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("dgraph.type", "string"), Predicate("name", "string"))), Some(UidRange(0, 5)))),
        Some(Partition(Seq(Target("localhost:9080")), Some(Set(Predicate("dgraph.type", "string"), Predicate("name", "string"))), Some(UidRange(5, 5))))
      )

      assert(partitions.length === expected.size)
      assert(partitions === expected)
    }

    it("should partition data") {
      val target = "localhost:9080"
      val partitions =
        spark
          .read
          .option(UidRangePartitionerUidsPerPartOption, "7")
          .dgraphTriples(target)
          .mapPartitions(part => Iterator(part.map(_.getLong(0)).toSet))
          .collect()
      assert(partitions.length === 2)
      assert(partitions === Seq((1 to 7).toSet, (8 to 10).toSet))
    }

  }

}
