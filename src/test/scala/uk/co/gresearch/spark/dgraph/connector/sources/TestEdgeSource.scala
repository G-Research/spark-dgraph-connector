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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, lit, row_number}
import org.apache.spark.sql.types.LongType
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark._
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.{DgraphCluster, DgraphTestCluster}

import scala.reflect.runtime.universe._

class TestEdgeSource extends AnyFunSpec with ShuffleExchangeTests
  with ConnectorSparkTestSession with DgraphTestCluster
  with FilterPushdownTestHelper
  with ProjectionPushDownTestHelper {

  import spark.implicits._

  describe("EdgeSource") {

    lazy val expecteds = EdgesSourceExpecteds(dgraph)
    lazy val expectedRows = expecteds.getExpectedEdgeDf(spark).collect().toSet
    lazy val expectedEdges = expecteds.getExpectedEdges

    def doTestLoadEdges(load: () => DataFrame): Unit = {
      val edges = load().collect().toSet
      assert(edges === expectedRows)
    }

    it("should load edges via path") {
      doTestLoadEdges(() =>
        reader
          .format(EdgesSource)
          .load(dgraph.target)
      )
    }

    it("should load edges via paths") {
      doTestLoadEdges(() =>
        reader
          .format(EdgesSource)
          .load(dgraph.target, dgraph.targetLocalIp)
      )
    }

    it("should load edges via target option") {
      doTestLoadEdges(() =>
        reader
          .format(EdgesSource)
          .option(TargetOption, dgraph.target)
          .load()
      )
    }

    it("should load edges via targets option") {
      doTestLoadEdges(() =>
        reader
          .format(EdgesSource)
          .option(TargetsOption, s"""["${dgraph.target}","${dgraph.targetLocalIp}"]""")
          .load()
      )
    }

    it("should load edges via implicit dgraph target") {
      doTestLoadEdges(() =>
        reader
          .dgraph.edges(dgraph.target)
      )
    }

    it("should load edges via implicit dgraph targets") {
      doTestLoadEdges(() =>
        reader
          .dgraph.edges(dgraph.target)
      )
    }

    it("should load edges in chunks") {
      doTestLoadEdges(() =>
        reader
          .options(Map(
            PartitionerOption -> PredicatePartitionerOption,
            PredicatePartitionerPredicatesOption -> "2",
            ChunkSizeOption -> "3"
          ))
          .dgraph.edges(dgraph.target)
      )
    }

    it("should encode Edge") {
      val rows =
        reader
          .format(EdgesSource)
          .load(dgraph.target)
          .as[Edge]
          .collectAsList()
      assert(rows.size() === 12)
    }

    it("should fail without target") {
      assertThrows[IllegalArgumentException] {
        reader
          .format(EdgesSource)
          .load()
      }
    }

    it("should load as a single partition") {
      val target = dgraph.target
      val targets = Seq(Target(target))
      val partitions =
        reader
          .option(PartitionerOption, SingletonPartitionerOption)
          .dgraph.edges(target)
          .rdd
          .partitions.flatMap {
            case p: DataSourceRDDPartition => p.inputPartitions
            case _ => Seq.empty
          }
      assert(partitions === Seq(Partition(targets).has(Set(Predicate("director", "uid"), Predicate("starring", "uid")))))
    }

    it("should load as a predicate partitions") {
      val target = dgraph.target
      val partitions =
        reader
          .option(PartitionerOption, PredicatePartitionerOption)
          .option(PredicatePartitionerPredicatesOption, "2")
          .dgraph.edges(target)
          .rdd
          .partitions.flatMap {
            case p: DataSourceRDDPartition => p.inputPartitions
            case _ => Seq.empty
          }

      val expected = Seq(
        Partition(Seq(Target(dgraph.target))).has(Set.empty, Set("director", "starring")).getAll
      )

      assert(partitions === expected)
    }

    it("should partition data") {
      val target = dgraph.target
      val partitions =
        reader
          .options(Map(
            PartitionerOption -> UidRangePartitionerOption,
            UidRangePartitionerUidsPerPartOption -> "2",
            UidRangePartitionerEstimatorOption -> MaxLeaseIdEstimatorOption,
            MaxLeaseIdEstimatorIdOption -> dgraph.highestUid.toString
          ))
          .dgraph.edges(target)
          .mapPartitions(part => Iterator(part.map(_.getLong(0)).toSet))
          .collect()

      // we retrieve partitions in chunks of 2 uids, if there are uids allocated but unused then we get partitions with less than 2 uids
      val uids = Set(dgraph.sw1, dgraph.sw2, dgraph.sw3).map(_.toInt)
      val expected = (1 to dgraph.highestUid.toInt).grouped(2).map(_.toSet intersect uids).toSeq
      assert(partitions === expected)
    }

    lazy val edges =
      reader
        .options(Map(
          PartitionerOption -> PredicatePartitionerOption,
          PredicatePartitionerPredicatesOption -> "2"
        ))
        .dgraph.edges(dgraph.target)
        .as[Edge]
    lazy val edgesSinglePredicatePerPartition =
      spark
        .read
        .options(Map(
          PartitionerOption -> PredicatePartitionerOption,
          PredicatePartitionerPredicatesOption -> "1"
        ))
        .dgraph.edges(dgraph.target)
        .as[Edge]

    it("should push subject filters") {
      doTestFilterPushDown(
        $"subject" === dgraph.leia,
        Set(SubjectIsIn(Uid(dgraph.leia))),
        expecteds = expectedEdges.filter(_.subject == dgraph.leia)
      )

      doTestFilterPushDown(
        $"subject".isin(dgraph.leia),
        Set(SubjectIsIn(Uid(dgraph.leia))),
        expecteds = expectedEdges.filter(_.subject == dgraph.leia)
      )

      doTestFilterPushDown(
        $"subject".isin(dgraph.leia, dgraph.luke),
        Set(SubjectIsIn(Uid(dgraph.leia), Uid(dgraph.luke))),
        expecteds = expectedEdges.filter(r => Set(dgraph.leia, dgraph.luke).contains(r.subject))
      )
    }

    it("should push predicate filters") {
      doTestFilterPushDown(
        $"predicate" === "director",
        Set(IntersectPredicateNameIsIn("director")),
        expecteds = expectedEdges.filter(_.predicate == "director")
      )

      doTestFilterPushDown(
        $"predicate".isin("director"),
        Set(IntersectPredicateNameIsIn("director")),
        expecteds = expectedEdges.filter(_.predicate == "director")
      )

      doTestFilterPushDown(
        $"predicate".isin("director", "starring"),
        Set(IntersectPredicateNameIsIn("director", "starring")),
        expecteds = expectedEdges.filter(r => Set("director", "starring").contains(r.predicate))
      )
    }

    it("should push object value filters") {
      doTestFilterPushDownDf(
        edges,
        $"objectUid" === dgraph.leia,
        Set(ObjectValueIsIn(dgraph.leia), ObjectTypeIsIn("uid")),
        // With multiple predicates per partition, we cannot filter for object values
        Seq(EqualTo(AttributeReference("objectUid", LongType, nullable = true)(), Literal(dgraph.leia))),
        expecteds = expectedEdges.filter(_.objectUid == dgraph.leia)
      )
      doTestFilterPushDownDf(
        edgesSinglePredicatePerPartition,
        $"objectUid" === dgraph.leia,
        Set(ObjectValueIsIn(dgraph.leia), ObjectTypeIsIn("uid")),
        expecteds = expectedEdges.filter(_.objectUid == dgraph.leia)
      )

      doTestFilterPushDownDf(
        edges,
        $"objectUid".isin(dgraph.leia),
        Set(ObjectValueIsIn(dgraph.leia), ObjectTypeIsIn("uid")),
        // With multiple predicates per partition, we cannot filter for object values
        Seq(EqualTo(AttributeReference("objectUid", LongType, nullable = true)(), Literal(dgraph.leia))),
        expecteds = expectedEdges.filter(_.objectUid == dgraph.leia)
      )
      doTestFilterPushDownDf(
        edgesSinglePredicatePerPartition,
        $"objectUid".isin(dgraph.leia),
        Set(ObjectValueIsIn(dgraph.leia), ObjectTypeIsIn("uid")),
        expecteds = expectedEdges.filter(_.objectUid == dgraph.leia)
      )

      doTestFilterPushDownDf(
        edges,
        $"objectUid".isin(dgraph.leia, dgraph.lucas),
        Set(ObjectValueIsIn(dgraph.leia, dgraph.lucas), ObjectTypeIsIn("uid")),
        // With multiple predicates per partition, we cannot filter for object values
        Seq(In(AttributeReference("objectUid", LongType, nullable = true)(), Seq(Literal(dgraph.leia), Literal(dgraph.lucas)))),
        expecteds = expectedEdges.filter(r => Set(dgraph.leia, dgraph.lucas).contains(r.objectUid))
      )
      doTestFilterPushDownDf(
        edgesSinglePredicatePerPartition,
        $"objectUid".isin(dgraph.leia, dgraph.lucas),
        Set(ObjectValueIsIn(dgraph.leia, dgraph.lucas), ObjectTypeIsIn("uid")),
        expecteds = expectedEdges.filter(r => Set(dgraph.leia, dgraph.lucas).contains(r.objectUid))
      )
    }

    def doTestFilterPushDown(condition: Column, expectedFilters: Set[Filter], expectedUnpushed: Seq[Expression] = Seq.empty, expecteds: Set[Edge]): Unit = {
      doTestFilterPushDownDf(edges, condition, expectedFilters, expectedUnpushed, expecteds)
    }

    it("should not push projection") {
      doTestProjectionPushDownDf(
        edges.toDF(),
        Seq($"subject", $"objectUid"),
        None,
        expectedRows.map(select(0, 2))
      )

      doTestProjectionPushDownDf(
        edges.toDF(),
        Seq($"subject", $"predicate", $"objectUid"),
        None,
        expectedRows
      )

      doTestProjectionPushDownDf(
        edges.toDF(),
        Seq.empty,
        None,
        expectedRows
      )
    }

    lazy val expectedPredicateCounts = expectedEdges.toSeq.groupBy(_.predicate)
      .mapValues(_.length).toSeq.sortBy(_._1).map(e => Row(e._1, e._2))
    val predicatePartitioningTests = Seq(
      ("distinct", (df: DataFrame) => df.select($"predicate").distinct(), () => expectedPredicateCounts.map(row => Row(row.getString(0)))),
      ("groupBy", (df: DataFrame) => df.groupBy($"predicate").count(), () => expectedPredicateCounts),
      ("Window.partitionBy", (df: DataFrame) => df.select($"predicate", count(lit(1)) over Window.partitionBy($"predicate")),
        () => expectedPredicateCounts.flatMap(row => row * row.getInt(1))  // all rows occur with cardinality of their count
      ),
      ("Window.partitionBy.orderBy", (df: DataFrame) => df.select($"predicate", row_number() over Window.partitionBy($"predicate").orderBy($"subject")),
        () => expectedPredicateCounts.flatMap(row => row ++ row.getInt(1))  // each row occurs with row_number up to their cardinality
      )
    )

    describe("without predicate partitioning") {
      val withoutPartitioning = () =>
        reader
          .options(Map(
            PartitionerOption -> s"$UidRangePartitionerOption",
            UidRangePartitionerUidsPerPartOption -> "2",
            MaxLeaseIdEstimatorIdOption -> dgraph.highestUid.toString
          ))
          .dgraph.edges(dgraph.target)

      testForShuffleExchange(withoutPartitioning, predicatePartitioningTests, shuffleExpected = true)
    }

    describe("with predicate partitioning") {
      val withPartitioning = () =>
        reader
          .option(PartitionerOption, PredicatePartitionerOption)
          .option(PredicatePartitionerPredicatesOption, "1")
          .dgraph.edges(dgraph.target)

      testForShuffleExchange(withPartitioning, predicatePartitioningTests, shuffleExpected = false)
    }

    lazy val expectedSubjectCounts = expectedEdges.toSeq.groupBy(_.subject)
      .mapValues(_.length).toSeq.sortBy(_._1).map(e => Row(e._1, e._2))
    val subjectPartitioningTests = Seq(
      ("distinct", (df: DataFrame) => df.select($"subject").distinct(), () => expectedSubjectCounts.map(row => Row(row.getLong(0)))),
      ("groupBy", (df: DataFrame) => df.groupBy($"subject").count(), () => expectedSubjectCounts),
      ("Window.partitionBy", (df: DataFrame) => df.select($"subject", count(lit(1)) over Window.partitionBy($"subject")),
        () => expectedSubjectCounts.flatMap(row => row * row.getInt(1))  // all rows occur with cardinality of their count
      ),
      ("Window.partitionBy.orderBy", (df: DataFrame) => df.select($"subject", row_number() over Window.partitionBy($"subject").orderBy($"predicate")),
        () => expectedSubjectCounts.flatMap(row => row ++ row.getInt(1))  // each row occurs with row_number up to their cardinality
      )
    )

    describe("without subject partitioning") {
      val withoutPartitioning = () =>
        reader
          .option(PartitionerOption, PredicatePartitionerOption)
          .option(PredicatePartitionerPredicatesOption, "1")
          .dgraph.edges(dgraph.target)

      testForShuffleExchange(withoutPartitioning, subjectPartitioningTests, shuffleExpected = true)
    }

    describe("with subject partitioning") {
      val withPartitioning = () =>
        reader
          .options(Map(
            PartitionerOption -> s"$UidRangePartitionerOption",
            UidRangePartitionerUidsPerPartOption -> "2",
            MaxLeaseIdEstimatorIdOption -> dgraph.highestUid.toString
          ))
          .dgraph.edges(dgraph.target)

      testForShuffleExchange(withPartitioning, subjectPartitioningTests, shuffleExpected = false)
    }

    lazy val expectedSubjectAndPredicateCounts = expectedEdges.toSeq.groupBy(t => (t.subject, t.predicate))
      .mapValues(_.length).toSeq.sortBy(_._1).map(e => Row(e._1._1, e._1._2, e._2))
    val subjectAndPredicatePartitioningTests = Seq(
      ("distinct", (df: DataFrame) => df.select($"subject", $"predicate").distinct(), () => expectedSubjectAndPredicateCounts.map(row => Row(row.getLong(0), row.getString(1)))),
      ("groupBy", (df: DataFrame) => df.groupBy($"subject", $"predicate").count(), () => expectedSubjectAndPredicateCounts),
      ("Window.partitionBy", (df: DataFrame) => df.select($"subject", $"predicate", count(lit(1)) over Window.partitionBy($"subject", $"predicate")),
        () => expectedSubjectAndPredicateCounts.flatMap(row => row * row.getInt(2))  // all rows occur with cardinality of their count
      ),
      ("Window.partitionBy.orderBy", (df: DataFrame) => df.select($"subject", $"predicate", row_number() over Window.partitionBy($"subject", $"predicate").orderBy($"objectUid")),
        () => expectedSubjectAndPredicateCounts.flatMap(row => row ++ row.getInt(2))  // each row occurs with row_number up to their cardinality
      )
    )

    describe("without subject and predicate partitioning") {
      val withoutPartitioning = () =>
        reader
          .option(PartitionerOption, PredicatePartitionerOption)
          .option(PredicatePartitionerPredicatesOption, "1")
          .dgraph.edges(dgraph.target)

      testForShuffleExchange(withoutPartitioning, subjectAndPredicatePartitioningTests, shuffleExpected = true)
    }

    describe("with subject and predicate partitioning") {
      val withPartitioning = () =>
        reader
          .options(Map(
            PartitionerOption -> s"$PredicatePartitionerOption+$UidRangePartitionerOption",
            PredicatePartitionerPredicatesOption -> "1",
            UidRangePartitionerUidsPerPartOption -> "2",
            MaxLeaseIdEstimatorIdOption -> dgraph.highestUid.toString
          ))
          .dgraph.edges(dgraph.target)

      testForShuffleExchange(withPartitioning, subjectAndPredicatePartitioningTests, shuffleExpected = false)
    }
  }

}

case class EdgesSourceExpecteds(cluster: DgraphCluster) {

  def getDataFrame[T <: Product : TypeTag](rows: Set[T], spark: SparkSession): DataFrame =
    spark.createDataset(rows.toSeq)(Encoders.product[T]).toDF()

  def getExpectedEdgeDf(spark: SparkSession): DataFrame =
    getDataFrame(getExpectedEdges, spark)(typeTag[Edge])

  def getExpectedEdges: Set[Edge] =
    Set(
      Edge(cluster.sw1, "director", cluster.lucas),
      Edge(cluster.sw1, "starring", cluster.leia),
      Edge(cluster.sw1, "starring", cluster.luke),
      Edge(cluster.sw1, "starring", cluster.han),
      Edge(cluster.sw2, "director", cluster.irvin),
      Edge(cluster.sw2, "starring", cluster.leia),
      Edge(cluster.sw2, "starring", cluster.luke),
      Edge(cluster.sw2, "starring", cluster.han),
      Edge(cluster.sw3, "director", cluster.richard),
      Edge(cluster.sw3, "starring", cluster.leia),
      Edge(cluster.sw3, "starring", cluster.luke),
      Edge(cluster.sw3, "starring", cluster.han),
    )

}
