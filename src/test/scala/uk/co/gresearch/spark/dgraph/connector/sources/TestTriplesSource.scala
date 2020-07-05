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

import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression, In, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, DataSourceV2ScanRelation}
import org.apache.spark.sql.types.StringType
import org.scalatest.{Assertions, FunSpec}
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.partitioner.PredicatePartitioner

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
        StringTriple(graphQlSchema, "dgraph.type", "dgraph.graphql", "string"),
        StringTriple(graphQlSchema, "dgraph.graphql.xid", "dgraph.graphql.schema", "string"),
        StringTriple(graphQlSchema, "dgraph.graphql.schema", "", "string"),
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

    it("should load string-object triples in chunks") {
      // it is hard to test data are really read in chunks, but we can test the data are correct
      doTestLoadStringTriples(() =>
        spark
          .read
          .options(Map(
            TriplesModeOption -> TriplesModeStringOption,
            PartitionerOption -> PredicatePartitionerOption,
            PredicatePartitionerPredicatesOption -> "2",
            ChunkSizeOption -> "3"
          ))
          .dgraphTriples(cluster.grpc)
      )
    }

    it("should load typed-object triples in chunks") {
      // it is hard to test data are really read in chunks, but we can test the data are correct
      doTestLoadTypedTriples(() =>
        spark
          .read
          .options(Map(
            TriplesModeOption -> TriplesModeTypedOption,
            PartitionerOption -> PredicatePartitionerOption,
            PredicatePartitionerPredicatesOption -> "2",
            ChunkSizeOption -> "3"
          ))
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
      assert(rows.size() == 47)
    }

    it("should encode TypedTriple") {
      val rows =
        spark
          .read
          .option(TriplesModeOption, TriplesModeTypedOption)
          .dgraphTriples(cluster.grpc)
          .as[TypedTriple]
          .collectAsList()
      assert(rows.size() == 47)
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
      val target = cluster.grpc
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
      assert(partitions === Seq(Some(Partition(targets, None, None))))
    }

    it("should load as predicate partitions") {
      val partitions =
        spark
          .read
          .option(PartitionerOption, PredicatePartitionerOption)
          .option(PredicatePartitionerPredicatesOption, "2")
          .dgraphTriples(cluster.grpc)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition => Some(p.inputPartition)
          case _ => None
        }

      val expected = Set(
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("release_date", "datetime"), Predicate("starring", "uid"))), None)),
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("revenue", "float"))), None)),
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("dgraph.graphql.schema", "string"), Predicate("running_time", "int"))), None)),
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("dgraph.type", "string"), Predicate("dgraph.graphql.xid", "string"))), None)),
        Some(Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("director", "uid"), Predicate("name", "string"))), None))
      )

      assert(partitions.toSet === expected)
    }

    it("should load as uid-range partitions") {
      val partitions =
        spark
          .read
          .options(Map(
            PartitionerOption -> s"$UidRangePartitionerOption",
            UidRangePartitionerUidsPerPartOption -> "7",
            MaxLeaseIdEstimatorIdOption -> highestUid.toString
          ))
          .dgraphTriples(cluster.grpc)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition => Some(p.inputPartition)
          case _ => None
        }

      val expected = Set(
        Some(Partition(Seq(Target(cluster.grpc)), None, Some(UidRange(Uid(1), Uid(8))))),
        Some(Partition(Seq(Target(cluster.grpc)), None, Some(UidRange(Uid(8), Uid(15))))),
      )

      assert(partitions.toSet === expected)
    }

    it("should load as predicate uid-range partitions") {
      val partitions =
        spark
          .read
          .options(Map(
            PartitionerOption -> s"$PredicatePartitionerOption+$UidRangePartitionerOption",
            PredicatePartitionerPredicatesOption -> "2",
            UidRangePartitionerUidsPerPartOption -> "5",
            MaxLeaseIdEstimatorIdOption -> highestUid.toString
          ))
          .dgraphTriples(cluster.grpc)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition => Some(p.inputPartition)
          case _ => None
        }

      val ranges = Seq(UidRange(Uid(1), Uid(6)), UidRange(Uid(6), Uid(11)), UidRange(Uid(11), Uid(16)))

      val expected = Set(
        Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("release_date", "datetime"), Predicate("starring", "uid"))), None),
        Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("revenue", "float"))), None),
        Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("dgraph.graphql.schema", "string"), Predicate("running_time", "int"))), None),
        Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("dgraph.type", "string"), Predicate("dgraph.graphql.xid", "string"))), None),
        Partition(Seq(Target(cluster.grpc)), Some(Set(Predicate("director", "uid"), Predicate("name", "string"))), None)
      ).flatMap(partition => ranges.map(range => Some(partition.copy(uids = Some(range)))))

      assert(partitions.toSet === expected)
    }

    it("should partition data") {
      val partitions =
        spark
          .read
          .options(Map(
            PartitionerOption -> UidRangePartitionerOption,
            UidRangePartitionerUidsPerPartOption -> "7",
            MaxLeaseIdEstimatorIdOption -> highestUid.toString
          ))
          .dgraphTriples(cluster.grpc)
          .mapPartitions(part => Iterator(part.map(_.getLong(0)).toSet))
          .collect()

      // ignore the existence or absence of graphQlSchema in the result, otherwise flaky test, see TestNodeSource
      assert(partitions.map(_ - graphQlSchema) === allUids.grouped(7).map(_.toSet - graphQlSchema).toSeq)
    }

    val triples =
      spark
        .read
        .option(PartitionerOption, PredicatePartitionerOption)
        .option(PredicatePartitionerPredicatesOption, "2")
        .dgraphTriples(cluster.grpc)

    it("should push predicate filters") {
      doTestFilterPushDown($"predicate" === "name", Seq(PredicateNameIsIn("name")))
      doTestFilterPushDown($"predicate".isin("name"), Seq(PredicateNameIsIn("name")))
      doTestFilterPushDown($"predicate".isin("name", "starring"), Seq(PredicateNameIsIn("name", "starring")))
    }

    it("should push object type filters") {
      doTestFilterPushDown($"objectType" === "string", Seq(ObjectTypeIsIn("string")))
      doTestFilterPushDown($"objectType".isin("string"), Seq(ObjectTypeIsIn("string")))
      doTestFilterPushDown($"objectType".isin("string", "uid"), Seq(ObjectTypeIsIn("string", "uid")))
    }

    it("should push object value filters") {
      doTestFilterPushDown(
        $"objectString" === "Person",
        Seq(ObjectValueIsIn("Person"), ObjectTypeIsIn("string")),
        Seq(
          IsNotNull(AttributeReference("objectString", StringType, nullable = true)()),
          EqualTo(AttributeReference("objectString", StringType, nullable = true)(), Literal("Person"))
        )
      )
      doTestFilterPushDown(
        $"objectString".isin("Person"),
        Seq(ObjectValueIsIn("Person"), ObjectTypeIsIn("string")),
        Seq(
          IsNotNull(AttributeReference("objectString", StringType, nullable = true)()),
          EqualTo(AttributeReference("objectString", StringType, nullable = true)(), Literal("Person"))
        )
      )
      doTestFilterPushDown(
        $"objectString".isin("Person", "Film"),
        Seq(ObjectValueIsIn("Person", "Film"), ObjectTypeIsIn("string")),
        Seq(
          In(AttributeReference("objectString", StringType, nullable = true)(), Seq(Literal("Person"), Literal("Film")))
        )
      )
      doTestFilterPushDown(
        $"objectString" === "Person" && $"objectUid" === 1,
        Seq(ObjectValueIsIn("Person"), ObjectTypeIsIn("string"), ObjectValueIsIn("1"), ObjectTypeIsIn("uid")),
        Seq(
          IsNotNull(AttributeReference("objectString", StringType, nullable = true)()),
          IsNotNull(AttributeReference("objectUid", StringType, nullable = true)()),
          EqualTo(AttributeReference("objectString", StringType, nullable = true)(), Literal("Person")),
          EqualTo(AttributeReference("objectUid", StringType, nullable = true)(), Literal(1L))
        )
      )
    }

    def doTestFilterPushDown(condition: Column, expected: Seq[Filter], expectedUnpushed: Seq[Expression]=Seq.empty): Unit = {
      val df =triples.where(condition)

      val plan = df.queryExecution.optimizedPlan
      val relationNode = plan match {
        case filter: logical.Filter =>
          val unpushedFilters = getFilterNodes(filter.condition)
          assert(unpushedFilters.map(_.sql) === expectedUnpushed.map(_.sql))
          filter.child
        case _ => plan
      }
      assert(relationNode.isInstanceOf[DataSourceV2ScanRelation])

      val relation = relationNode.asInstanceOf[DataSourceV2ScanRelation]
      assert(relation.scan.isInstanceOf[TripleScan])

      val scan = relation.scan.asInstanceOf[TripleScan]
      assert(scan.partitioner.isInstanceOf[PredicatePartitioner])

      val partitioner = scan.partitioner.asInstanceOf[PredicatePartitioner]
      assert(partitioner.filters.toSet === expected.toSet)
    }

    def getFilterNodes(node: Expression): Seq[Expression] = node match {
      case And(left, right) => getFilterNodes(left) ++ getFilterNodes(right)
      case _ => Seq(node)
    }

  }

}

object TestTriplesSource extends Assertions {

  def doAssertTriples(triples: Set[TypedTriple], cluster: DgraphTestCluster): Unit = {
    val expected = Set(
      TypedTriple(cluster.graphQlSchema, "dgraph.type", None, Some("dgraph.graphql"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.graphQlSchema, "dgraph.graphql.xid", None, Some("dgraph.graphql.schema"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.graphQlSchema, "dgraph.graphql.schema", None, Some(""), None, None, None, None, None, None, "string"),
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
