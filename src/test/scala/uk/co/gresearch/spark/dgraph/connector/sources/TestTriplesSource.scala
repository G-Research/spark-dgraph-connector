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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.scalatest.{Assertions, FunSpec}
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.encoder.TypedTripleEncoder
import uk.co.gresearch.spark.dgraph.connector.model.TripleTableModel

class TestTriplesSource extends FunSpec
  with SparkTestSession with DgraphTestCluster {

  import spark.implicits._

  describe("TriplesDataSource") {

    def doTestLoadTypedTriples(load: () => DataFrame): Unit = {
      val triples = load().as[TypedTriple].collect().toSet
      TestTriplesSource.doAssertTriples(triples, this)
    }

    def doTestLoadStringTriples(load: () => DataFrame): Unit = {
      val triples = load().as[StringTriple].collect().toSet
      val expected = Set(
        StringTriple(st1, "dgraph.type", "Film", "string"),
        StringTriple(st1, "name", "Star Trek: The Motion Picture", "string"),
        StringTriple(st1, "release_date", "1979-12-07 00:00:00.0", "timestamp"),
        StringTriple(st1, "revenue", "1.39E8", "double"),
        StringTriple(st1, "running_time", "132", "long"),
        StringTriple(leia, "dgraph.type", "Person", "string"),
        StringTriple(leia, "name", "Princess Leia", "string"),
        StringTriple(lucas, "dgraph.type", "Person", "string"),
        StringTriple(lucas, "name", "George Lucas", "string"),
        StringTriple(irvin, "dgraph.type", "Person", "string"),
        StringTriple(irvin, "name", "Irvin Kernshner", "string"),
        StringTriple(sw1, "dgraph.type", "Film", "string"),
        StringTriple(sw1, "director", lucas.toString, "uid"),
        StringTriple(sw1, "name", "Star Wars: Episode IV - A New Hope", "string"),
        StringTriple(sw1, "release_date", "1977-05-25 00:00:00.0", "timestamp"),
        StringTriple(sw1, "revenue", "7.75E8", "double"),
        StringTriple(sw1, "running_time", "121", "long"),
        StringTriple(sw1, "starring", leia.toString, "uid"),
        StringTriple(sw1, "starring", luke.toString, "uid"),
        StringTriple(sw1, "starring", han.toString, "uid"),
        StringTriple(sw2, "dgraph.type", "Film", "string"),
        StringTriple(sw2, "director", irvin.toString, "uid"),
        StringTriple(sw2, "name", "Star Wars: Episode V - The Empire Strikes Back", "string"),
        StringTriple(sw2, "release_date", "1980-05-21 00:00:00.0", "timestamp"),
        StringTriple(sw2, "revenue", "5.34E8", "double"),
        StringTriple(sw2, "running_time", "124", "long"),
        StringTriple(sw2, "starring", leia.toString, "uid"),
        StringTriple(sw2, "starring", luke.toString, "uid"),
        StringTriple(sw2, "starring", han.toString, "uid"),
        StringTriple(luke, "dgraph.type", "Person", "string"),
        StringTriple(luke, "name", "Luke Skywalker", "string"),
        StringTriple(han, "dgraph.type", "Person", "string"),
        StringTriple(han, "name", "Han Solo", "string"),
        StringTriple(richard, "dgraph.type", "Person", "string"),
        StringTriple(richard, "name", "Richard Marquand", "string"),
        StringTriple(sw3, "dgraph.type", "Film", "string"),
        StringTriple(sw3, "director", richard.toString, "uid"),
        StringTriple(sw3, "name", "Star Wars: Episode VI - Return of the Jedi", "string"),
        StringTriple(sw3, "release_date", "1983-05-25 00:00:00.0", "timestamp"),
        StringTriple(sw3, "revenue", "5.72E8", "double"),
        StringTriple(sw3, "running_time", "131", "long"),
        StringTriple(sw3, "starring", leia.toString, "uid"),
        StringTriple(sw3, "starring", luke.toString, "uid"),
        StringTriple(sw3, "starring", han.toString, "uid"),
      )
      assert(triples === expected)
    }

    it("should load triples via path") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .format(TriplesSource)
          .load(cluster.grpc)
      )
    }

    it("should load triples via paths") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .format(TriplesSource)
          .load(cluster.grpc, cluster.grpcLocalIp)
      )
    }

    it("should load triples via target option") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .format(TriplesSource)
          .option(TargetOption, cluster.grpc)
          .load()
      )
    }

    it("should load triples via targets option") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .format(TriplesSource)
          .option(TargetsOption, s"""["${cluster.grpc}","${cluster.grpcLocalIp}"]""")
          .load()
      )
    }

    it("should load triples via implicit dgraph target") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .dgraphTriples(cluster.grpc)
      )
    }

    it("should load triples via implicit dgraph targets") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .dgraphTriples(cluster.grpc, cluster.grpcLocalIp)
      )
    }

    it("should load string-object triples") {
      doTestLoadStringTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeStringOption)
          .dgraphTriples(cluster.grpc)
      )
    }

    it("should load typed-object triples") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeTypedOption)
          .dgraphTriples(cluster.grpc)
      )
    }

    it("should encode StringTriple") {
      val rows =
        spark
          .read
          .option(TriplesModeOption, TriplesModeStringOption)
          .dgraphTriples(cluster.grpc)
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
          .dgraphTriples(cluster.grpc)
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
      val target = cluster.grpc
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
      assert(partitions === Seq(Some(Partition(targets, None, None, model))))
    }

    it("should load as predicate partitions") {
      val target = cluster.grpc
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
      assert(partitions === Seq(
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("release_date", "datetime"), Predicate("revenue", "float"))), None, model)),
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("dgraph.graphql.schema", "string"), Predicate("starring", "uid"))), None, model)),
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("director", "uid"), Predicate("running_time", "int"))), None, model)),
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("dgraph.type", "string"), Predicate("name", "string"))), None, model))
      ))
    }

    it("should load as uid-range partitions") {
      val target = cluster.grpc
      val partitions =
        spark
          .read
          .option(PartitionerOption, s"$UidRangePartitionerOption")
          .option(UidRangePartitionerUidsPerPartOption, "7")
          .dgraphTriples(target)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition[_] => Some(p.inputPartition)
          case _ => None
        }

      val expected = Seq(
        Some(Partition(Seq(Target(cluster.grpc)), None, Some(UidRange(0, 7)), model)),
        Some(Partition(Seq(Target(cluster.grpc)), None, Some(UidRange(7, 7)), model)),
      )

      assert(partitions === expected)
    }

    it("should load as predicate uid-range partitions") {
      val target = cluster.grpc
      val partitions =
        spark
          .read
          .option(PartitionerOption, s"$PredicatePartitionerOption+$UidRangePartitionerOption")
          .option(PredicatePartitionerPredicatesOption, "2")
          .option(UidRangePartitionerUidsPerPartOption, "5")
          .dgraphTriples(target)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition[_] => Some(p.inputPartition)
          case _ => None
        }

      val expected = Seq(
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("release_date", "datetime"), Predicate("revenue", "float"))), None, model)),
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("dgraph.graphql.schema", "string"), Predicate("starring", "uid"))), None, model)),
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("director", "uid"), Predicate("running_time", "int"))), None, model)),
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("dgraph.type", "string"), Predicate("name", "string"))), Some(UidRange(0, 5)), model)),
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("dgraph.type", "string"), Predicate("name", "string"))), Some(UidRange(5, 5)), model))
      )

      assert(partitions === expected)
    }

    it("should partition data") {
      val target = cluster.grpc
      val partitions =
        spark
          .read
          .options(Map(
            PartitionerOption -> UidRangePartitionerOption,
            UidRangePartitionerUidsPerPartOption -> "7"
          ))
          .dgraphTriples(target)
          .mapPartitions(part => Iterator(part.map(_.getLong(0)).toSet))
          .collect()
      assert(partitions === Seq((1 to 7).toSet, (8 to 10).toSet))
    }

  }

}

object TestTriplesSource extends Assertions {

  def doAssertTriples(triples: Set[TypedTriple], cluster: DgraphTestCluster): Unit = {
    val expected = Set(
      TypedTriple(cluster.st1, "dgraph.type", None, Some("Film"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.st1, "name", None, Some("Star Trek: The Motion Picture"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.st1, "release_date", None, None, None, None, Some(Timestamp.valueOf("1979-12-07 00:00:00.0")), None, None, None, "timestamp"),
      TypedTriple(cluster.st1, "revenue", None, None, None, Some(1.39E8), None, None, None, None, "double"),
      TypedTriple(cluster.st1, "running_time", None, None, Some(132), None, None, None, None, None, "long"),
      TypedTriple(cluster.leia, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.leia, "name", None, Some("Princess Leia"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.lucas, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.lucas, "name", None, Some("George Lucas"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.irvin, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.irvin, "name", None, Some("Irvin Kernshner"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw1, "dgraph.type", None, Some("Film"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw1, "director", Some(cluster.lucas), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.sw1, "name", None, Some("Star Wars: Episode IV - A New Hope"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw1, "release_date", None, None, None, None, Some(Timestamp.valueOf("1977-05-25 00:00:00.0")), None, None, None, "timestamp"),
      TypedTriple(cluster.sw1, "revenue", None, None, None, Some(7.75E8), None, None, None, None, "double"),
      TypedTriple(cluster.sw1, "running_time", None, None, Some(121), None, None, None, None, None, "long"),
      TypedTriple(cluster.sw1, "starring", Some(cluster.leia), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.sw1, "starring", Some(cluster.luke), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.sw1, "starring", Some(cluster.han), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.sw2, "dgraph.type", None, Some("Film"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw2, "director", Some(cluster.irvin), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.sw2, "name", None, Some("Star Wars: Episode V - The Empire Strikes Back"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw2, "release_date", None, None, None, None, Some(Timestamp.valueOf("1980-05-21 00:00:00.0")), None, None, None, "timestamp"),
      TypedTriple(cluster.sw2, "revenue", None, None, None, Some(5.34E8), None, None, None, None, "double"),
      TypedTriple(cluster.sw2, "running_time", None, None, Some(124), None, None, None, None, None, "long"),
      TypedTriple(cluster.sw2, "starring", Some(cluster.leia), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.sw2, "starring", Some(cluster.luke), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.sw2, "starring", Some(cluster.han), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.luke, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.luke, "name", None, Some("Luke Skywalker"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.han, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.han, "name", None, Some("Han Solo"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.richard, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.richard, "name", None, Some("Richard Marquand"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw3, "dgraph.type", None, Some("Film"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw3, "director", Some(cluster.richard), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.sw3, "name", None, Some("Star Wars: Episode VI - Return of the Jedi"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw3, "release_date", None, None, None, None, Some(Timestamp.valueOf("1983-05-25 00:00:00.0")), None, None, None, "timestamp"),
      TypedTriple(cluster.sw3, "revenue", None, None, None, Some(5.72E8), None, None, None, None, "double"),
      TypedTriple(cluster.sw3, "running_time", None, None, Some(131), None, None, None, None, None, "long"),
      TypedTriple(cluster.sw3, "starring", Some(cluster.leia), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.sw3, "starring", Some(cluster.luke), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.sw3, "starring", Some(cluster.han), None, None, None, None, None, None, None, "uid"),
    )
    assert(triples === expected)
  }

}
