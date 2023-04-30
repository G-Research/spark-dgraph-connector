/*
 * Copyright 2020 G-Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector.sources

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, In, Literal}
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.apache.spark.sql.types.{LongType, StringType}
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark._
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.sources.TestTriplesSource.removeDgraphTriples
import uk.co.gresearch.spark.dgraph.{DgraphCluster, DgraphTestCluster}

import java.sql.Timestamp
import scala.reflect.runtime.universe._

class TestTriplesSource extends AnyFunSpec
  with ConnectorSparkTestSession with DgraphTestCluster
  with FilterPushdownTestHelper
  with ProjectionPushDownTestHelper {

  import spark.implicits._

  describe("TriplesDataSource") {

    lazy val expecteds = TriplesSourceExpecteds(dgraph)
    lazy val expectedTypedTriples = expecteds.getExpectedTypedTriples
    lazy val expectedStringTriples = expecteds.getExpectedStringTriples

    def doTestLoadTypedTriples(load: () => DataFrame, expected: Set[TypedTriple] = expectedTypedTriples, removeDgraph: Boolean = true): Unit = {
      val triples = (if (removeDgraph) removeDgraphTriples(load().as[TypedTriple]) else load().as[TypedTriple]).collect().toSet
      assert(triples === expected)
    }

    def doTestLoadStringTriples(load: () => DataFrame, expected: Set[StringTriple] = expectedStringTriples, removeDgraph: Boolean = true): Unit = {
      val triples = (if (removeDgraph) removeDgraphTriples(load().as[StringTriple]) else load().as[StringTriple]).collect().toSet
      assert(triples === expected)
    }

    val predicates = Set(
      Predicate("dgraph.type", "string"),
      Predicate("director", "uid"),
      Predicate("name", "string"),
      Predicate("title", "string", isLang = true),
      Predicate("release_date", "datetime"),
      Predicate("revenue", "float"),
      Predicate("running_time", "int"),
      Predicate("starring", "uid")
    )

    it("should load triples via path") {
      doTestLoadTypedTriples(() =>
        reader
          .format(TriplesSource)
          .load(dgraph.target)
      )
    }

    it("should load triples via paths") {
      doTestLoadTypedTriples(() =>
        reader
          .format(TriplesSource)
          .load(dgraph.target, dgraph.targetLocalIp)
      )
    }

    it("should load triples via target option") {
      doTestLoadTypedTriples(() =>
        reader
          .format(TriplesSource)
          .option(TargetOption, dgraph.target)
          .load()
      )
    }

    it("should load triples via targets option") {
      doTestLoadTypedTriples(() =>
        reader
          .format(TriplesSource)
          .option(TargetsOption, s"""["${dgraph.target}","${dgraph.targetLocalIp}"]""")
          .load()
      )
    }

    it("should load triples via implicit dgraph target") {
      doTestLoadTypedTriples(() =>
        reader
          .dgraph.triples(dgraph.target)
      )
    }

    it("should load triples via implicit dgraph targets") {
      doTestLoadTypedTriples(() =>
        reader
          .dgraph.triples(dgraph.target)
      )
    }

    it("should load string-object triples") {
      doTestLoadStringTriples(() =>
        reader
          .option(TriplesModeOption, TriplesModeStringOption)
          .dgraph.triples(dgraph.target)
      )
    }

    it("should load typed-object triples") {
      doTestLoadTypedTriples(() =>
        reader
          .option(TriplesModeOption, TriplesModeTypedOption)
          .dgraph.triples(dgraph.target)
      )
    }

    lazy val availableStringTriples =
      spark
        .read
        .option(TriplesModeOption, TriplesModeStringOption)
        .dgraph.triples(dgraph.target)
        .as[StringTriple]
        .collect()
        .toSet

    // These reserved predicate tests assume some reserved predicates exist in the test Dgraph data
    // See TestDgraphTestCluster for those assumptions
    it("should load string-object triples and include reserved predicates") {
      doTestLoadStringTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeStringOption)
          .option(IncludeReservedPredicatesOption, "dgraph.type")
          .dgraph.triples(dgraph.target),
        availableStringTriples.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate == "dgraph.type"),
        removeDgraph = false
      )

      doTestLoadStringTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeStringOption)
          .option(IncludeReservedPredicatesOption, "dgraph.type,dgraph.graphql.xid")
          .dgraph.triples(dgraph.target),
        availableStringTriples.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate == "dgraph.type" || triple.predicate == "dgraph.graphql.xid"),
        removeDgraph = false
      )

      doTestLoadStringTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeStringOption)
          .option(IncludeReservedPredicatesOption, "dgraph.graphql.*")
          .dgraph.triples(dgraph.target),
        availableStringTriples.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate.startsWith("dgraph.graphql.")),
        removeDgraph = false
      )

      doTestLoadStringTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeStringOption)
          .option(IncludeReservedPredicatesOption, "dgraph.type,dgraph.graphql.*")
          .dgraph.triples(dgraph.target),
        availableStringTriples.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate == "dgraph.type" || triple.predicate.startsWith("dgraph.graphql.")),
        removeDgraph = false
      )
    }

    it("should load string-object triples and exclude reserved predicates") {
      doTestLoadStringTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeStringOption)
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.xid")
          .dgraph.triples(dgraph.target),
        availableStringTriples.filter(triple => triple.predicate != "dgraph.graphql.xid"),
        removeDgraph = false
      )

      doTestLoadStringTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeStringOption)
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.schema,dgraph.graphql.xid")
          .dgraph.triples(dgraph.target),
        availableStringTriples.filter(triple => !Set("dgraph.graphql.schema", "dgraph.graphql.xid").contains(triple.predicate)),
        removeDgraph = false
      )

      doTestLoadStringTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeStringOption)
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.*")
          .dgraph.triples(dgraph.target),
        availableStringTriples.filter(triple => !triple.predicate.startsWith("dgraph.graphql.")),
        removeDgraph = false
      )

      doTestLoadStringTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeStringOption)
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.*,dgraph.type")
          .dgraph.triples(dgraph.target),
        availableStringTriples.filter(triple => !triple.predicate.startsWith("dgraph.graphql.") && triple.predicate != "dgraph.type"),
        removeDgraph = false
      )
    }

    it("should load string-object triples and include/exclude reserved predicates") {
      doTestLoadStringTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeStringOption)
          .option(IncludeReservedPredicatesOption, "dgraph.graphql.*")
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.xid")
          .dgraph.triples(dgraph.target),
        availableStringTriples.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate.startsWith("dgraph.graphql.") && triple.predicate != "dgraph.graphql.xid"),
        removeDgraph = false
      )

      doTestLoadStringTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeStringOption)
          .option(IncludeReservedPredicatesOption, "dgraph.graphql.*")
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.x*")
          .dgraph.triples(dgraph.target),
        availableStringTriples.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate.startsWith("dgraph.graphql.") && !triple.predicate.startsWith("dgraph.graphql.x")),
        removeDgraph = false
      )
    }

    lazy val availableTypedTriples =
      spark
        .read
        .option(TriplesModeOption, TriplesModeTypedOption)
        .dgraph.triples(dgraph.target)
        .as[TypedTriple]
        .collect()
        .toSet

    it("should load typed-object triples and include reserved predicates") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeTypedOption)
          .option(IncludeReservedPredicatesOption, "dgraph.type")
          .dgraph.triples(dgraph.target),
        availableTypedTriples.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate == "dgraph.type"),
        removeDgraph = false
      )

      doTestLoadTypedTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeTypedOption)
          .option(IncludeReservedPredicatesOption, "dgraph.type,dgraph.graphql.xid")
          .dgraph.triples(dgraph.target),
        availableTypedTriples.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate == "dgraph.type" || triple.predicate == "dgraph.graphql.xid"),
        removeDgraph = false
      )

      doTestLoadTypedTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeTypedOption)
          .option(IncludeReservedPredicatesOption, "dgraph.graphql.*")
          .dgraph.triples(dgraph.target),
        availableTypedTriples.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate.startsWith("dgraph.graphql.")),
        removeDgraph = false
      )

      doTestLoadTypedTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeTypedOption)
          .option(IncludeReservedPredicatesOption, "dgraph.type,dgraph.graphql.*")
          .dgraph.triples(dgraph.target),
        availableTypedTriples.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate == "dgraph.type" || triple.predicate.startsWith("dgraph.graphql.")),
        removeDgraph = false
      )
    }

    it("should load typed-object triples and exclude reserved predicates") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeTypedOption)
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.xid")
          .dgraph.triples(dgraph.target),
        availableTypedTriples.filter(triple => triple.predicate != "dgraph.graphql.xid"),
        removeDgraph = false
      )

      doTestLoadTypedTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeTypedOption)
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.schema,dgraph.graphql.xid")
          .dgraph.triples(dgraph.target),
        availableTypedTriples.filter(triple => !Set("dgraph.graphql.schema", "dgraph.graphql.xid").contains(triple.predicate)),
        removeDgraph = false
      )

      doTestLoadTypedTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeTypedOption)
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.*")
          .dgraph.triples(dgraph.target),
        availableTypedTriples.filter(triple => !triple.predicate.startsWith("dgraph.graphql.")),
        removeDgraph = false
      )

      doTestLoadTypedTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeTypedOption)
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.*,dgraph.type")
          .dgraph.triples(dgraph.target),
        availableTypedTriples.filter(triple => !triple.predicate.startsWith("dgraph.graphql.") && triple.predicate != "dgraph.type"),
        removeDgraph = false
      )
    }

    it("should load typed-object triples and include/exclude reserved predicates") {
      doTestLoadTypedTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeTypedOption)
          .option(IncludeReservedPredicatesOption, "dgraph.graphql.*")
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.xid")
          .dgraph.triples(dgraph.target),
        availableTypedTriples.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate.startsWith("dgraph.graphql.") && triple.predicate != "dgraph.graphql.xid"),
        removeDgraph = false
      )

      doTestLoadTypedTriples(() =>
        spark
          .read
          .option(TriplesModeOption, TriplesModeTypedOption)
          .option(IncludeReservedPredicatesOption, "dgraph.graphql.*")
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.x*")
          .dgraph.triples(dgraph.target),
        availableTypedTriples.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate.startsWith("dgraph.graphql.") && !triple.predicate.startsWith("dgraph.graphql.x")),
        removeDgraph = false
      )
    }

    it("should load string-object triples in chunks") {
      // it is hard to test data are really read in chunks, but we can test the data are correct
      doTestLoadStringTriples(() =>
        reader
          .options(Map(
            TriplesModeOption -> TriplesModeStringOption,
            PartitionerOption -> PredicatePartitionerOption,
            PredicatePartitionerPredicatesOption -> "2",
            ChunkSizeOption -> "3"
          ))
          .dgraph.triples(dgraph.target)
      )
    }

    it("should load typed-object triples in chunks") {
      // it is hard to test data are really read in chunks, but we can test the data are correct
      doTestLoadTypedTriples(() =>
        reader
          .options(Map(
            TriplesModeOption -> TriplesModeTypedOption,
            PartitionerOption -> PredicatePartitionerOption,
            PredicatePartitionerPredicatesOption -> "2",
            ChunkSizeOption -> "3"
          ))
          .dgraph.triples(dgraph.target)
      )
    }

    it("should encode StringTriple") {
      val rows =
        removeDgraphTriples(
          reader
            .option(TriplesModeOption, TriplesModeStringOption)
            .dgraph.triples(dgraph.target)
            .as[StringTriple]
        ).collectAsList()
      assert(rows.size() == 61)
    }

    it("should encode TypedTriple") {
      val rows =
        removeDgraphTriples(
          reader
            .option(TriplesModeOption, TriplesModeTypedOption)
            .dgraph.triples(dgraph.target)
            .as[TypedTriple]
        ).collectAsList()
      assert(rows.size() == 61)
    }

    it("should fail without target") {
      assertThrows[IllegalArgumentException] {
        reader
          .format(TriplesSource)
          .load()
      }
    }

    it("should fail with unknown triple mode") {
      assertThrows[IllegalArgumentException] {
        reader
          .format(TriplesSource)
          .option(TriplesModeOption, "unknown")
          .load()
      }
    }

    it("should load as a single partition") {
      val target = dgraph.target
      val targets = Seq(Target(target))
      val partitions =
        reader
          .option(PartitionerOption, SingletonPartitionerOption)
          .dgraph.triples(target)
          .rdd
          .partitions.flatMap {
            case p: DataSourceRDDPartition => p.inputPartitions
            case _ => Seq.empty
          }
      assert(partitions === Seq(Partition(targets).has(predicates).langs(Set("title"))))
    }

    it("should load as predicate partitions") {
      val partitions =
        reader
          .option(PartitionerOption, PredicatePartitionerOption)
          .option(PredicatePartitionerPredicatesOption, "2")
          .dgraph.triples(dgraph.target)
          .rdd
          .partitions.flatMap {
            case p: DataSourceRDDPartition => p.inputPartitions
            case _ => Seq.empty
          }

      val expected = Seq(
        Partition(Seq(Target(dgraph.target))).has(Set("release_date"), Set("starring")).getAll,
        Partition(Seq(Target(dgraph.target))).has(Set("running_time"), Set("director")).getAll,
        Partition(Seq(Target(dgraph.target))).has(Set("dgraph.type", "name"), Set.empty).getAll,
        Partition(Seq(Target(dgraph.target))).has(Set("revenue", "title"), Set.empty).langs(Set("title")).getAll
      )

      assert(partitions === expected)
    }

    it("should load as uid-range partitions") {
      val partitions =
        reader
          .options(Map(
            PartitionerOption -> s"$UidRangePartitionerOption",
            UidRangePartitionerUidsPerPartOption -> "7",
            MaxLeaseIdEstimatorIdOption -> dgraph.highestUid.toString
          ))
          .dgraph.triples(dgraph.target)
          .rdd
          .partitions.flatMap {
            case p: DataSourceRDDPartition => p.inputPartitions
            case _ => Seq.empty
          }

      val expected = Seq(
        Partition(Seq(Target(dgraph.target)), Set(Has(predicates), LangDirective(Set("title")), UidRange(Uid(1), Uid(8)))),
        Partition(Seq(Target(dgraph.target)), Set(Has(predicates), LangDirective(Set("title")), UidRange(Uid(8), Uid(15))))
      )

      assert(partitions === expected)
    }

    it("should load as predicate uid-range partitions") {
      val partitions =
        reader
          .options(Map(
            PartitionerOption -> s"$PredicatePartitionerOption+$UidRangePartitionerOption",
            PredicatePartitionerPredicatesOption -> "2",
            UidRangePartitionerUidsPerPartOption -> "5",
            MaxLeaseIdEstimatorIdOption -> dgraph.highestUid.toString
          ))
          .dgraph.triples(dgraph.target)
          .rdd
          .partitions.flatMap {
            case p: DataSourceRDDPartition => p.inputPartitions
            case _ => Seq.empty
          }

      val ranges = Seq(UidRange(Uid(1), Uid(6)), UidRange(Uid(6), Uid(11)), UidRange(Uid(11), Uid(16)))

      val expected = Seq(
        Partition(Seq(Target(dgraph.target))).has(Set("release_date"), Set("starring")).getAll,
        Partition(Seq(Target(dgraph.target))).has(Set("running_time"), Set("director")).getAll,
        Partition(Seq(Target(dgraph.target))).has(Set("dgraph.type", "name"), Set.empty).getAll,
        Partition(Seq(Target(dgraph.target))).has(Set("revenue", "title"), Set.empty).langs(Set("title")).getAll
      ).flatMap(partition => ranges.map(range => partition.copy(operators = partition.operators + range)))

      assert(partitions === expected)
    }

    it("should partition data") {
      val partitions = {
        removeDgraphTriples(
          reader
            .options(Map(
              PartitionerOption -> UidRangePartitionerOption,
              UidRangePartitionerUidsPerPartOption -> "7",
              MaxLeaseIdEstimatorIdOption -> dgraph.highestUid.toString
            ))
            .dgraph.triples(dgraph.target)
        )
          .mapPartitions(part => Iterator(part.map(_.getLong(0)).toSet))
          .collect()
      }

      // we retrieve partitions in chunks of 7 uids, if there are uids allocated but unused then we get partitions with less than 7 uids
      val allUidInts = dgraph.allUids.map(_.toInt).toSet
      val expected = (1 to dgraph.highestUid.toInt).grouped(7).map(_.toSet intersect allUidInts).toSeq
      assert(partitions === expected)
    }

    lazy val typedTriples =
      removeDgraphTriples(
        reader
          .option(TriplesModeOption, TriplesModeTypedOption)
          .option(PartitionerOption, PredicatePartitionerOption)
          .option(PredicatePartitionerPredicatesOption, "2")
          .dgraph.triples(dgraph.target)
          .as[TypedTriple]
      )

    lazy val stringTriples =
      removeDgraphTriples(
        reader
          .option(TriplesModeOption, TriplesModeStringOption)
          .option(PartitionerOption, PredicatePartitionerOption)
          .option(PredicatePartitionerPredicatesOption, "2")
          .dgraph.triples(dgraph.target)
          .as[StringTriple]
      )

    lazy val typedTriplesSinglePredicatePartitions =
      removeDgraphTriples(
        reader
          .option(TriplesModeOption, TriplesModeTypedOption)
          .option(PartitionerOption, PredicatePartitionerOption)
          .option(PredicatePartitionerPredicatesOption, "1")
          .dgraph.triples(dgraph.target)
          .as[TypedTriple]
      )

    lazy val stringTriplesSinglePredicatePartitions =
      removeDgraphTriples(
        reader
          .option(TriplesModeOption, TriplesModeStringOption)
          .option(PartitionerOption, PredicatePartitionerOption)
          .option(PredicatePartitionerPredicatesOption, "1")
          .dgraph.triples(dgraph.target)
          .as[StringTriple]
      )

    it("should push subject filters") {
      doTestFilterPushDown(
        $"subject" === dgraph.leia,
        Set(SubjectIsIn(Uid(dgraph.leia))), Seq.empty,
        (t: TypedTriple) => t.subject.equals(dgraph.leia),
        (t: StringTriple) => t.subject.equals(dgraph.leia)
      )

      doTestFilterPushDown(
        $"subject".isin(dgraph.leia),
        Set(SubjectIsIn(Uid(dgraph.leia))), Seq.empty,
        (t: TypedTriple) => t.subject.equals(dgraph.leia),
        (t: StringTriple) => t.subject.equals(dgraph.leia)
      )

      doTestFilterPushDown(
        $"subject".isin(dgraph.leia, dgraph.luke),
        Set(SubjectIsIn(Uid(dgraph.leia), Uid(dgraph.luke))), Seq.empty,
        (t: TypedTriple) => t.subject.equals(dgraph.leia) || t.subject.equals(dgraph.luke),
        (t: StringTriple) => t.subject.equals(dgraph.leia) || t.subject.equals(dgraph.luke)
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
        // With multiple predicates per partition, we cannot filter for object values
        Seq(EqualTo(AttributeReference("objectString", StringType, nullable = true)(), Literal("Person"))),
        expectedDs = expectedTypedTriples.filter(_.objectString.exists(_.equals("Person")))
      )
      doTestFilterPushDownDf(typedTriplesSinglePredicatePartitions,
        $"objectString" === "Person",
        Set(ObjectValueIsIn("Person"), ObjectTypeIsIn("string")),
        expectedDs = expectedTypedTriples.filter(_.objectString.exists(_.equals("Person")))
      )

      doTestFilterPushDownDf(typedTriples,
        $"objectString".isin("Person"),
        Set(ObjectValueIsIn("Person"), ObjectTypeIsIn("string")),
        // With multiple predicates per partition, we cannot filter for object values
        Seq(EqualTo(AttributeReference("objectString", StringType, nullable = true)(), Literal("Person"))),
        expectedDs = expectedTypedTriples.filter(_.objectString.exists(_.equals("Person")))
      )
      doTestFilterPushDownDf(typedTriples,
        $"objectString".isin("Person", "Film"),
        Set(ObjectValueIsIn("Person", "Film"), ObjectTypeIsIn("string")),
        // With multiple predicates per partition, we cannot filter for object values
        Seq(In(AttributeReference("objectString", StringType, nullable = true)(), Seq(Literal("Person"), Literal("Film")))),
        expectedDs = expectedTypedTriples.filter(_.objectString.exists(Set("Person", "Film").contains))
      )
      doTestFilterPushDownDf(typedTriplesSinglePredicatePartitions,
        $"objectString".isin("Person"),
        Set(ObjectValueIsIn("Person"), ObjectTypeIsIn("string")),
        expectedDs = expectedTypedTriples.filter(_.objectString.exists(_.equals("Person")))
      )
      doTestFilterPushDownDf(typedTriplesSinglePredicatePartitions,
        $"objectString".isin("Person", "Film"),
        Set(ObjectValueIsIn("Person", "Film"), ObjectTypeIsIn("string")),
        expectedDs = expectedTypedTriples.filter(_.objectString.exists(Set("Person", "Film").contains))
      )

      doTestFilterPushDownDf(typedTriples,
        $"objectString" === "Person" && $"objectUid" === 1,
        Set(AlwaysFalse),
        // With multiple predicates per partition, we cannot filter for object values
        Seq(
          EqualTo(AttributeReference("objectString", StringType, nullable = true)(), Literal("Person")),
          EqualTo(AttributeReference("objectUid", LongType, nullable = true)(), Literal(1L))
        ),
        expectedDs = Set.empty
      )
      doTestFilterPushDownDf(typedTriplesSinglePredicatePartitions,
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

    // The 'should push object value filters with predicate name filters for ... triples' tests
    // test a bug that surfaced when one of multiple predicates in a partition matches
    // the object value, then the other predicates are retrieved for those uids as well
    // no matter if they match
    // so we restrict to exactly two predicates here, where only one matches 'Person'
    it("should push object value filters with predicate name filters for typed triples") {
      doTestFilterPushDownDf(typedTriples,
        $"objectString" === "Person" && $"predicate".isin("dgraph.type", "name"),
        Set(ObjectTypeIsIn("string"), ObjectValueIsIn("Person"), IntersectPredicateNameIsIn("dgraph.type", "name")),
        // With multiple predicates per partition, we cannot filter for object values
        // Spark will have to filter on top
        Seq(
          EqualTo(AttributeReference("objectString", StringType, nullable = true)(), Literal("Person"))
        ),
        expectedDs = expectedTypedTriples.filter(_.objectString.exists(_.equals("Person")))
      )

      doTestFilterPushDownDf(typedTriplesSinglePredicatePartitions,
        $"objectString" === "Person" && $"predicate".isin("dgraph.type", "name"),
        Set(IntersectPredicateValueIsIn(Set("dgraph.type", "name"), Set("Person")), ObjectTypeIsIn("string")),
        expectedDs = expectedTypedTriples.filter(_.objectString.exists(_.equals("Person")))
      )
    }

    it("should push object value filters with predicate name filters for string triples") {
      doTestFilterPushDownDf(stringTriples,
        $"objectString".isin("Person") && $"predicate".isin("dgraph.type", "name"),
        Set(IntersectPredicateNameIsIn(Set("dgraph.type", "name")), ObjectValueIsIn("Person")),
        // With multiple predicates per partition, we cannot filter for object values
        // The partitioner will push some value filters to dgraph
        // but Spark will have to filter on top
        Seq(
          EqualTo(AttributeReference("objectString", StringType, nullable = true)(), Literal("Person"))
        ),
        expectedDs = expectedStringTriples.filter(_.objectString.equals("Person")).filter(_.predicate.equals("dgraph.type"))
      )
      doTestFilterPushDownDf(stringTriplesSinglePredicatePartitions,
        $"objectString".isin("Person") && $"predicate".isin("dgraph.type", "name"),
        Set(IntersectPredicateValueIsIn(Set("dgraph.type", "name"), Set("Person"))),
        // Even with a single predicate per partition, we cannot tell
        // TableScanBuilder that EqualTo("objectString") is actually being done
        // by ObjectValueIsIn("Person") and ObjectTypeIsIn("string")
        // so the partitioner will efficiently perform the EqualTo("objectString")
        // but Spark will still filter on top, which is fine
        Seq(
          EqualTo(AttributeReference("objectString", StringType, nullable = true)(), Literal("Person"))
        ),
        expectedDs = expectedStringTriples.filter(_.objectString.equals("Person")).filter(_.predicate.equals("dgraph.type"))
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


    it("should not push projection") {
      doTestProjectionPushDownDf(typedTriples.toDF(),
        Seq($"subject", $"predicate", $"objectUid", $"objectString"),
        None,
        expectedTypedTriples.toSeq.toDF().collect().toSet.map(select(0, 1, 2, 3))
      )

      doTestProjectionPushDownDf(stringTriples.toDF(),
        Seq($"subject", $"predicate", $"objectString"),
        None,
        expectedStringTriples.toSeq.toDF().collect().toSet.map(select(0, 1, 2))
      )
    }

    Seq(1, 3, 5).foreach( partitions =>
      it(f"should work with first partitioner and $partitions partitions") {
        val triples =
          reader
            .options(Map(
              PartitionerOption -> f"$FirstPartitionerOption+$PredicatePartitionerOption+$UidRangePartitionerOption",
              FirstPartitionerPartitionsOption -> partitions.toString,
              PredicatePartitionerPredicatesOption -> "4",
              UidRangePartitionerUidsPerPartOption -> "4",
              ChunkSizeOption -> "2",
              MaxLeaseIdEstimatorIdOption -> dgraph.highestUid.toString
            ))
            .dgraph.triples(dgraph.target)
            .mapPartitions(it => Iterator.single(it.length))
            .count
        assert(triples === partitions)
      }
    )

  }

}

case class TriplesSourceExpecteds(cluster: DgraphCluster) {

  def getDataFrame[T <: Product : TypeTag](rows: Set[T], spark: SparkSession): DataFrame =
    spark.createDataset(rows.toSeq)(Encoders.product[T]).toDF()

  def getExpectedTypedTripleDf(spark: SparkSession): DataFrame =
    getDataFrame(getExpectedTypedTriples, spark)(typeTag[TypedTriple])

  def getExpectedTypedTriples: Set[TypedTriple] =
    Set(
      TypedTriple(cluster.st1, "dgraph.type", None, Some("Film"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.st1, "title@en", None, Some("Star Trek: The Motion Picture"), None, None, None, None, None, None, "string"),
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
      TypedTriple(cluster.sw1, "title", None, Some("Star Wars: Episode IV - A New Hope"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw1, "title@en", None, Some("Star Wars: Episode IV - A New Hope"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw1, "title@hu", None, Some("Csillagok háborúja IV: Egy új remény"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw1, "title@be", None, Some("Зорныя войны. Эпізод IV: Новая надзея"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw1, "title@cs", None, Some("Star Wars: Epizoda IV – Nová naděje"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw1, "title@br", None, Some("Star Wars Lodenn 4: Ur Spi Nevez"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw1, "title@de", None, Some("Krieg der Sterne"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw1, "release_date", None, None, None, None, Some(Timestamp.valueOf("1977-05-25 00:00:00.0")), None, None, None, "timestamp"),
      TypedTriple(cluster.sw1, "revenue", None, None, None, Some(7.75E8), None, None, None, None, "double"),
      TypedTriple(cluster.sw1, "running_time", None, None, Some(121), None, None, None, None, None, "long"),
      TypedTriple(cluster.sw1, "starring", Some(cluster.leia), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.sw1, "starring", Some(cluster.luke), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.sw1, "starring", Some(cluster.han), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.sw2, "dgraph.type", None, Some("Film"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw2, "director", Some(cluster.irvin), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.sw2, "title", None, Some("Star Wars: Episode V - The Empire Strikes Back"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw2, "title@en", None, Some("Star Wars: Episode V - The Empire Strikes Back"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw2, "title@ka", None, Some("ვარსკვლავური ომები, ეპიზოდი V: იმპერიის საპასუხო დარტყმა"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw2, "title@ko", None, Some("제국의 역습"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw2, "title@iw", None, Some("מלחמת הכוכבים - פרק 5: האימפריה מכה שנית"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw2, "title@de", None, Some("Das Imperium schlägt zurück"), None, None, None, None, None, None, "string"),
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
      TypedTriple(cluster.sw3, "title", None, Some("Star Wars: Episode VI - Return of the Jedi"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw3, "title@en", None, Some("Star Wars: Episode VI - Return of the Jedi"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw3, "title@zh", None, Some("星際大戰六部曲：絕地大反攻"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw3, "title@th", None, Some("สตาร์ วอร์ส เอพพิโซด 6: การกลับมาของเจได"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw3, "title@fa", None, Some("بازگشت جدای"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw3, "title@ar", None, Some("حرب النجوم الجزء السادس: عودة الجيداي"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw3, "title@de", None, Some("Die Rückkehr der Jedi-Ritter"), None, None, None, None, None, None, "string"),
      TypedTriple(cluster.sw3, "release_date", None, None, None, None, Some(Timestamp.valueOf("1983-05-25 00:00:00.0")), None, None, None, "timestamp"),
      TypedTriple(cluster.sw3, "revenue", None, None, None, Some(5.72E8), None, None, None, None, "double"),
      TypedTriple(cluster.sw3, "running_time", None, None, Some(131), None, None, None, None, None, "long"),
      TypedTriple(cluster.sw3, "starring", Some(cluster.leia), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.sw3, "starring", Some(cluster.luke), None, None, None, None, None, None, None, "uid"),
      TypedTriple(cluster.sw3, "starring", Some(cluster.han), None, None, None, None, None, None, None, "uid"),
    )

  def getExpectedStringTripleDf(spark: SparkSession): DataFrame =
    getDataFrame(getExpectedStringTriples, spark)(typeTag[StringTriple])

  def getExpectedStringTriples: Set[StringTriple] =
    Set(
      StringTriple(cluster.st1, "dgraph.type", "Film", "string"),
      StringTriple(cluster.st1, "title@en", "Star Trek: The Motion Picture", "string"),
      StringTriple(cluster.st1, "release_date", "1979-12-07 00:00:00.0", "timestamp"),
      StringTriple(cluster.st1, "revenue", "1.39E8", "double"),
      StringTriple(cluster.st1, "running_time", "132", "long"),
      StringTriple(cluster.leia, "dgraph.type", "Person", "string"),
      StringTriple(cluster.leia, "name", "Princess Leia", "string"),
      StringTriple(cluster.lucas, "dgraph.type", "Person", "string"),
      StringTriple(cluster.lucas, "name", "George Lucas", "string"),
      StringTriple(cluster.irvin, "dgraph.type", "Person", "string"),
      StringTriple(cluster.irvin, "name", "Irvin Kernshner", "string"),
      StringTriple(cluster.sw1, "dgraph.type", "Film", "string"),
      StringTriple(cluster.sw1, "director", cluster.lucas.toString, "uid"),
      StringTriple(cluster.sw1, "title", "Star Wars: Episode IV - A New Hope", "string"),
      StringTriple(cluster.sw1, "title@en", "Star Wars: Episode IV - A New Hope", "string"),
      StringTriple(cluster.sw1, "title@hu", "Csillagok háborúja IV: Egy új remény", "string"),
      StringTriple(cluster.sw1, "title@be", "Зорныя войны. Эпізод IV: Новая надзея", "string"),
      StringTriple(cluster.sw1, "title@cs", "Star Wars: Epizoda IV – Nová naděje", "string"),
      StringTriple(cluster.sw1, "title@br", "Star Wars Lodenn 4: Ur Spi Nevez", "string"),
      StringTriple(cluster.sw1, "title@de", "Krieg der Sterne", "string"),
      StringTriple(cluster.sw1, "release_date", "1977-05-25 00:00:00.0", "timestamp"),
      StringTriple(cluster.sw1, "revenue", "7.75E8", "double"),
      StringTriple(cluster.sw1, "running_time", "121", "long"),
      StringTriple(cluster.sw1, "starring", cluster.leia.toString, "uid"),
      StringTriple(cluster.sw1, "starring", cluster.luke.toString, "uid"),
      StringTriple(cluster.sw1, "starring", cluster.han.toString, "uid"),
      StringTriple(cluster.sw2, "dgraph.type", "Film", "string"),
      StringTriple(cluster.sw2, "director", cluster.irvin.toString, "uid"),
      StringTriple(cluster.sw2, "title", "Star Wars: Episode V - The Empire Strikes Back", "string"),
      StringTriple(cluster.sw2, "title@en", "Star Wars: Episode V - The Empire Strikes Back", "string"),
      StringTriple(cluster.sw2, "title@ka", "ვარსკვლავური ომები, ეპიზოდი V: იმპერიის საპასუხო დარტყმა", "string"),
      StringTriple(cluster.sw2, "title@ko", "제국의 역습", "string"),
      StringTriple(cluster.sw2, "title@iw", "מלחמת הכוכבים - פרק 5: האימפריה מכה שנית", "string"),
      StringTriple(cluster.sw2, "title@de", "Das Imperium schlägt zurück", "string"),
      StringTriple(cluster.sw2, "release_date", "1980-05-21 00:00:00.0", "timestamp"),
      StringTriple(cluster.sw2, "revenue", "5.34E8", "double"),
      StringTriple(cluster.sw2, "running_time", "124", "long"),
      StringTriple(cluster.sw2, "starring", cluster.leia.toString, "uid"),
      StringTriple(cluster.sw2, "starring", cluster.luke.toString, "uid"),
      StringTriple(cluster.sw2, "starring", cluster.han.toString, "uid"),
      StringTriple(cluster.luke, "dgraph.type", "Person", "string"),
      StringTriple(cluster.luke, "name", "Luke Skywalker", "string"),
      StringTriple(cluster.han, "dgraph.type", "Person", "string"),
      StringTriple(cluster.han, "name", "Han Solo", "string"),
      StringTriple(cluster.richard, "dgraph.type", "Person", "string"),
      StringTriple(cluster.richard, "name", "Richard Marquand", "string"),
      StringTriple(cluster.sw3, "dgraph.type", "Film", "string"),
      StringTriple(cluster.sw3, "director", cluster.richard.toString, "uid"),
      StringTriple(cluster.sw3, "title", "Star Wars: Episode VI - Return of the Jedi", "string"),
      StringTriple(cluster.sw3, "title@en", "Star Wars: Episode VI - Return of the Jedi", "string"),
      StringTriple(cluster.sw3, "title@zh", "星際大戰六部曲：絕地大反攻", "string"),
      StringTriple(cluster.sw3, "title@th", "สตาร์ วอร์ส เอพพิโซด 6: การกลับมาของเจได", "string"),
      StringTriple(cluster.sw3, "title@fa", "بازگشت جدای", "string"),
      StringTriple(cluster.sw3, "title@ar", "حرب النجوم الجزء السادس: عودة الجيداي", "string"),
      StringTriple(cluster.sw3, "title@de", "Die Rückkehr der Jedi-Ritter", "string"),
      StringTriple(cluster.sw3, "release_date", "1983-05-25 00:00:00.0", "timestamp"),
      StringTriple(cluster.sw3, "revenue", "5.72E8", "double"),
      StringTriple(cluster.sw3, "running_time", "131", "long"),
      StringTriple(cluster.sw3, "starring", cluster.leia.toString, "uid"),
      StringTriple(cluster.sw3, "starring", cluster.luke.toString, "uid"),
      StringTriple(cluster.sw3, "starring", cluster.han.toString, "uid"),
    )

}

object TestTriplesSource {

  def removeDgraphTriples[T](triples: Dataset[T]): Dataset[T] = {
    import triples.sparkSession.implicits._
    val dgraphNodeUids = triples.where($"predicate" === "dgraph.type" && $"objectString".startsWith("dgraph.")).select($"subject").distinct().as[Long].collect()
    triples.where(!$"subject".isin(dgraphNodeUids: _*))
  }

}
