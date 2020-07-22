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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, In, Literal}
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame}
import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector._

class TestTriplesSource extends FunSpec
  with SparkTestSession with DgraphTestCluster
  with FilterPushDownTestHelper {

  import spark.implicits._

  describe("TriplesDataSource") {

    lazy val expectedTypedTriples = Set(
      TypedTriple(graphQlSchema, "dgraph.type", None, Some("dgraph.graphql"), None, None, None, None, None, None, "string"),
      TypedTriple(graphQlSchema, "dgraph.graphql.xid", None, Some("dgraph.graphql.schema"), None, None, None, None, None, None, "string"),
      TypedTriple(graphQlSchema, "dgraph.graphql.schema", None, Some(""), None, None, None, None, None, None, "string"),
      TypedTriple(st1, "dgraph.type", None, Some("Film"), None, None, None, None, None, None, "string"),
      TypedTriple(st1, "name", None, Some("Star Trek: The Motion Picture"), None, None, None, None, None, None, "string"),
      TypedTriple(st1, "release_date", None, None, None, None, Some(Timestamp.valueOf("1979-12-07 00:00:00.0")), None, None, None, "timestamp"),
      TypedTriple(st1, "revenue", None, None, None, Some(1.39E8), None, None, None, None, "double"),
      TypedTriple(st1, "running_time", None, None, Some(132), None, None, None, None, None, "long"),
      TypedTriple(leia, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
      TypedTriple(leia, "name", None, Some("Princess Leia"), None, None, None, None, None, None, "string"),
      TypedTriple(lucas, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
      TypedTriple(lucas, "name", None, Some("George Lucas"), None, None, None, None, None, None, "string"),
      TypedTriple(irvin, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
      TypedTriple(irvin, "name", None, Some("Irvin Kernshner"), None, None, None, None, None, None, "string"),
      TypedTriple(sw1, "dgraph.type", None, Some("Film"), None, None, None, None, None, None, "string"),
      TypedTriple(sw1, "director", Some(lucas), None, None, None, None, None, None, None, "uid"),
      TypedTriple(sw1, "name", None, Some("Star Wars: Episode IV - A New Hope"), None, None, None, None, None, None, "string"),
      TypedTriple(sw1, "release_date", None, None, None, None, Some(Timestamp.valueOf("1977-05-25 00:00:00.0")), None, None, None, "timestamp"),
      TypedTriple(sw1, "revenue", None, None, None, Some(7.75E8), None, None, None, None, "double"),
      TypedTriple(sw1, "running_time", None, None, Some(121), None, None, None, None, None, "long"),
      TypedTriple(sw1, "starring", Some(leia), None, None, None, None, None, None, None, "uid"),
      TypedTriple(sw1, "starring", Some(luke), None, None, None, None, None, None, None, "uid"),
      TypedTriple(sw1, "starring", Some(han), None, None, None, None, None, None, None, "uid"),
      TypedTriple(sw2, "dgraph.type", None, Some("Film"), None, None, None, None, None, None, "string"),
      TypedTriple(sw2, "director", Some(irvin), None, None, None, None, None, None, None, "uid"),
      TypedTriple(sw2, "name", None, Some("Star Wars: Episode V - The Empire Strikes Back"), None, None, None, None, None, None, "string"),
      TypedTriple(sw2, "release_date", None, None, None, None, Some(Timestamp.valueOf("1980-05-21 00:00:00.0")), None, None, None, "timestamp"),
      TypedTriple(sw2, "revenue", None, None, None, Some(5.34E8), None, None, None, None, "double"),
      TypedTriple(sw2, "running_time", None, None, Some(124), None, None, None, None, None, "long"),
      TypedTriple(sw2, "starring", Some(leia), None, None, None, None, None, None, None, "uid"),
      TypedTriple(sw2, "starring", Some(luke), None, None, None, None, None, None, None, "uid"),
      TypedTriple(sw2, "starring", Some(han), None, None, None, None, None, None, None, "uid"),
      TypedTriple(luke, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
      TypedTriple(luke, "name", None, Some("Luke Skywalker"), None, None, None, None, None, None, "string"),
      TypedTriple(han, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
      TypedTriple(han, "name", None, Some("Han Solo"), None, None, None, None, None, None, "string"),
      TypedTriple(richard, "dgraph.type", None, Some("Person"), None, None, None, None, None, None, "string"),
      TypedTriple(richard, "name", None, Some("Richard Marquand"), None, None, None, None, None, None, "string"),
      TypedTriple(sw3, "dgraph.type", None, Some("Film"), None, None, None, None, None, None, "string"),
      TypedTriple(sw3, "director", Some(richard), None, None, None, None, None, None, None, "uid"),
      TypedTriple(sw3, "name", None, Some("Star Wars: Episode VI - Return of the Jedi"), None, None, None, None, None, None, "string"),
      TypedTriple(sw3, "release_date", None, None, None, None, Some(Timestamp.valueOf("1983-05-25 00:00:00.0")), None, None, None, "timestamp"),
      TypedTriple(sw3, "revenue", None, None, None, Some(5.72E8), None, None, None, None, "double"),
      TypedTriple(sw3, "running_time", None, None, Some(131), None, None, None, None, None, "long"),
      TypedTriple(sw3, "starring", Some(leia), None, None, None, None, None, None, None, "uid"),
      TypedTriple(sw3, "starring", Some(luke), None, None, None, None, None, None, None, "uid"),
      TypedTriple(sw3, "starring", Some(han), None, None, None, None, None, None, None, "uid"),
    )

    def doTestLoadTypedTriples(load: () => DataFrame): Unit = {
      val triples = load().as[TypedTriple].collect().toSet
      assert(triples === expectedTypedTriples)
    }

    lazy val expectedStringTriples = Set(
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

    def doTestLoadStringTriples(load: () => DataFrame): Unit = {
      val triples = load().as[StringTriple].collect().toSet
      assert(triples === expectedStringTriples)
    }

    val predicates = Set(
      Predicate("dgraph.graphql.schema", "string"),
      Predicate("dgraph.graphql.xid", "string"),
      Predicate("dgraph.type", "string"),
      Predicate("director", "uid"),
      Predicate("name", "string"),
      Predicate("release_date", "datetime"),
      Predicate("revenue", "float"),
      Predicate("running_time", "int"),
      Predicate("starring", "uid")
    )

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
      assert(partitions === Seq(Some(Partition(targets).has(predicates))))
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
        Some(Partition(Seq(Target(cluster.grpc))).has(Set("release_date"), Set("starring")).getAll),
        Some(Partition(Seq(Target(cluster.grpc))).has(Set("revenue"), Set.empty).getAll),
        Some(Partition(Seq(Target(cluster.grpc))).has(Set("dgraph.graphql.schema", "running_time"), Set.empty).getAll),
        Some(Partition(Seq(Target(cluster.grpc))).has(Set("dgraph.type", "dgraph.graphql.xid"), Set.empty).getAll),
        Some(Partition(Seq(Target(cluster.grpc))).has(Set("name"), Set("director")).getAll)
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
        Some(Partition(Seq(Target(cluster.grpc)), Set(Has(predicates), UidRange(Uid(1), Uid(8))))),
        Some(Partition(Seq(Target(cluster.grpc)), Set(Has(predicates), UidRange(Uid(8), Uid(15))))),
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
        Partition(Seq(Target(cluster.grpc))).has(Set("release_date"), Set("starring")).getAll,
        Partition(Seq(Target(cluster.grpc))).has(Set("revenue"), Set.empty).getAll,
        Partition(Seq(Target(cluster.grpc))).has(Set("dgraph.graphql.schema", "running_time"), Set.empty).getAll,
        Partition(Seq(Target(cluster.grpc))).has(Set("dgraph.type", "dgraph.graphql.xid"), Set.empty).getAll,
        Partition(Seq(Target(cluster.grpc))).has(Set("name"), Set("director")).getAll
      ).flatMap(partition => ranges.map(range => Some(partition.copy(operators = partition.operators ++ Set(range)))))

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

      // we retrieve partitions in chunks of 7 uids, if there are uids allocated but unused then we get partitions with less than 7 uids
      val allUidInts = allUids.map(_.toInt).toSet
      val expected = (1 to highestUid.toInt).grouped(7).map(_.toSet intersect allUidInts).toSeq
      assert(partitions === expected)
    }

    lazy val typedTriples =
      spark
        .read
        .option(TriplesModeOption, TriplesModeTypedOption)
        .option(PartitionerOption, PredicatePartitionerOption)
        .option(PredicatePartitionerPredicatesOption, "2")
        .dgraphTriples(cluster.grpc)
        .as[TypedTriple]

    lazy val stringTriples =
      spark
        .read
        .option(TriplesModeOption, TriplesModeStringOption)
        .option(PartitionerOption, PredicatePartitionerOption)
        .option(PredicatePartitionerPredicatesOption, "2")
        .dgraphTriples(cluster.grpc)
        .as[StringTriple]

    it("should push subject filters") {
      doTestFilterPushDown(
        $"subject" === leia,
        Set(SubjectIsIn(Uid(leia))), Seq.empty,
        (t: TypedTriple) => t.subject.equals(leia),
        (t: StringTriple) => t.subject.equals(leia)
      )

      doTestFilterPushDown(
        $"subject".isin(leia),
        Set(SubjectIsIn(Uid(leia))), Seq.empty,
        (t: TypedTriple) => t.subject.equals(leia),
        (t: StringTriple) => t.subject.equals(leia)
      )

      doTestFilterPushDown(
        $"subject".isin(leia, luke),
        Set(SubjectIsIn(Uid(leia), Uid(luke))), Seq.empty,
        (t: TypedTriple) => t.subject.equals(leia) || t.subject.equals(luke),
        (t: StringTriple) => t.subject.equals(leia) || t.subject.equals(luke)
      )
    }

    it("should push predicate filters") {
      doTestFilterPushDown(
        $"predicate" === "name",
        Set(IntersectPredicateNameIsIn("name")), Seq.empty,
        (t: TypedTriple) => t.predicate.equals("name"),
        (t: StringTriple) => t.predicate.equals("name")
      )
      doTestFilterPushDown(
        $"predicate".isin("name"),
        Set(IntersectPredicateNameIsIn("name")),
        Seq.empty,
        (t: TypedTriple) => t.predicate.equals("name"),
        (t: StringTriple) => t.predicate.equals("name")
      )
      doTestFilterPushDown(
        $"predicate".isin("name", "starring"),
        Set(IntersectPredicateNameIsIn("name", "starring")),
        Seq.empty,
        (t: TypedTriple) => Set("name", "starring").contains(t.predicate),
        (t: StringTriple) => Set("name", "starring").contains(t.predicate)
      )
    }

    it("should push object type filters") {
      doTestFilterPushDown(
        $"objectType" === "string",
        Set(ObjectTypeIsIn("string")),
        Seq.empty,
        (t: TypedTriple) => t.objectType.equals("string"),
        (t: StringTriple) => t.objectType.equals("string")
      )
      doTestFilterPushDown(
        $"objectType".isin("string"),
        Set(ObjectTypeIsIn("string")),
        Seq.empty,
        (t: TypedTriple) => t.objectType.equals("string"),
        (t: StringTriple) => t.objectType.equals("string")
      )
      doTestFilterPushDown(
        $"objectType".isin("string", "uid"),
        Set(ObjectTypeIsIn("string", "uid")),
        Seq.empty,
        (t: TypedTriple) => Set("string", "uid").contains(t.objectType),
        (t: StringTriple) => Set("string", "uid").contains(t.objectType)
      )
    }

    it("should push object value filters for typed triples") {
      doTestFilterPushDownDf(typedTriples,
        $"objectString".isNotNull,
        Set(ObjectTypeIsIn("string")),
        expectedDs = expectedTypedTriples.filter(_.objectString.isDefined)
      )
      doTestFilterPushDownDf(typedTriples,
        $"objectString".isNotNull && $"objectUid".isNotNull,
        Set(AlwaysFalse),
        expectedDs = Set.empty
      )

      doTestFilterPushDownDf(typedTriples,
        $"objectString" === "Person",
        Set(ObjectValueIsIn("Person"), ObjectTypeIsIn("string")),
        expectedDs = expectedTypedTriples.filter(_.objectString.exists(_.equals("Person")))
      )

      doTestFilterPushDownDf(typedTriples,
        $"objectString".isin("Person"),
        Set(ObjectValueIsIn("Person"), ObjectTypeIsIn("string")),
        expectedDs = expectedTypedTriples.filter(_.objectString.exists(_.equals("Person")))
      )
      doTestFilterPushDownDf(typedTriples,
        $"objectString".isin("Person", "Film"),
        Set(ObjectValueIsIn("Person", "Film"), ObjectTypeIsIn("string")),
        expectedDs = expectedTypedTriples.filter(_.objectString.exists(Set("Person", "Film").contains))
      )

      doTestFilterPushDownDf(typedTriples,
        $"objectString" === "Person" && $"objectUid" === 1,
        Set(AlwaysFalse),
        expectedDs = Set.empty
      )
    }

    it("should push object value filters for string triples") {
      doTestFilterPushDownDf(stringTriples,
        $"objectString" === "Person",
        Set(ObjectValueIsIn("Person")),
        Seq(
          EqualTo(AttributeReference("objectString", StringType, nullable = true)(), Literal("Person"))
        ),
        expectedDs = expectedStringTriples.filter(_.objectString.equals("Person"))
      )
      doTestFilterPushDownDf(stringTriples,
        $"objectString" === "Person" && $"objectType" === "string",
        Set(ObjectValueIsIn("Person"), ObjectTypeIsIn("string")),
        // TableScanBuilder cannot know that EqualTo("objectString") is actually being done by ObjectValueIsIn("Person") and ObjectTypeIsIn("string")
        // the partitioner will efficiently read but spark will still filter on top, which is fine
        Seq(
          EqualTo(AttributeReference("objectString", StringType, nullable = true)(), Literal("Person"))
        ),
        expectedDs = expectedStringTriples.filter(_.objectString.equals("Person")).filter(_.objectType.equals("string"))
      )

      doTestFilterPushDownDf(stringTriples,
        $"objectString".isin("Person"),
        Set(ObjectValueIsIn("Person")),
        Seq(
          EqualTo(AttributeReference("objectString", StringType, nullable = true)(), Literal("Person"))
        ),
        expectedDs = expectedStringTriples.filter(_.objectString.equals("Person"))
      )
      doTestFilterPushDownDf(stringTriples,
        $"objectString".isin("Person") && $"objectType" === "string",
        Set(ObjectValueIsIn("Person"), ObjectTypeIsIn("string")),
        // TableScanBuilder cannot know that EqualTo("objectString") is actually being done by ObjectValueIsIn("Person") and ObjectTypeIsIn("string")
        // the partitioner will efficiently read but spark will still filter on top, which is fine
        Seq(
          EqualTo(AttributeReference("objectString", StringType, nullable = true)(), Literal("Person"))
        ),
        expectedDs = expectedStringTriples.filter(_.objectString.equals("Person")).filter(_.objectType.equals("string"))
      )

      doTestFilterPushDownDf(stringTriples,
        $"objectString".isin("Person", "Film"),
        Set(ObjectValueIsIn("Person", "Film")),
        Seq(
          In(AttributeReference("objectString", StringType, nullable = true)(), Seq(Literal("Person"), Literal("Film")))
        ),
        expectedDs = expectedStringTriples.filter(t => Set("Person", "Film").contains(t.objectString))
      )
      doTestFilterPushDownDf(stringTriples,
        $"objectString".isin("Person", "Film") && $"objectType" === "string",
        Set(ObjectValueIsIn("Person", "Film"), ObjectTypeIsIn("string")),
        // TableScanBuilder cannot know that EqualTo("objectString") is actually being done by ObjectValueIsIn("Person") and ObjectTypeIsIn("string")
        // the partitioner will efficiently read but spark will still filter on top, which is fine
        Seq(
          In(AttributeReference("objectString", StringType, nullable = true)(), Seq(Literal("Person"), Literal("Film")))
        ),
        expectedDs = expectedStringTriples.filter(t => Set("Person", "Film").contains(t.objectString) && t.objectType.equals("string"))
      )
    }

    def doTestFilterPushDown(condition: Column,
                             expectedFilters: Set[Filter],
                             expectedUnpushed: Seq[Expression],
                             expectedTypedDsFilter: TypedTriple => Boolean,
                             expectedStringDsFilter: StringTriple => Boolean): Unit = {
      doTestFilterPushDownDf(typedTriples, condition, expectedFilters, expectedUnpushed, expectedTypedTriples.filter(expectedTypedDsFilter))
      doTestFilterPushDownDf(stringTriples, condition, expectedFilters, expectedUnpushed, expectedStringTriples.filter(expectedStringDsFilter))
    }

  }

}
