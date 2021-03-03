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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, In, Literal}
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{Column, DataFrame, Encoders, Row, SparkSession}
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.encoder.EdgeEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.DgraphExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.model.EdgeTableModel
import uk.co.gresearch.spark.dgraph.{DgraphCluster, DgraphTestCluster}

import scala.reflect.runtime.universe._

class TestEdgeSource extends AnyFunSpec
  with SparkTestSession with DgraphTestCluster
  with FilterPushdownTestHelper
  with ProjectionPushDownTestHelper {

  import spark.implicits._

  describe("EdgeDataSource") {

    lazy val expecteds = EdgesSourceExpecteds(dgraph)
    lazy val expectedEdges = expecteds.getExpectedEdgeDf(spark).collect().toSet

    def doTestLoadEdges(load: () => DataFrame): Unit = {
      val edges = load().collect().toSet
      assert(edges === expectedEdges)
    }

    it("should load edges via path") {
      doTestLoadEdges(() =>
        spark
          .read
          .format(EdgesSource)
          .load(dgraph.target)
      )
    }

    it("should load edges via paths") {
      doTestLoadEdges(() =>
        spark
          .read
          .format(EdgesSource)
          .load(dgraph.target, dgraph.targetLocalIp)
      )
    }

    it("should load edges via target option") {
      doTestLoadEdges(() =>
        spark
          .read
          .format(EdgesSource)
          .option(TargetOption, dgraph.target)
          .load()
      )
    }

    it("should load edges via targets option") {
      doTestLoadEdges(() =>
        spark
          .read
          .format(EdgesSource)
          .option(TargetsOption, s"""["${dgraph.target}","${dgraph.targetLocalIp}"]""")
          .load()
      )
    }

    it("should load edges via implicit dgraph target") {
      doTestLoadEdges(() =>
        spark
          .read
          .dgraph.edges(dgraph.target)
      )
    }

    it("should load edges via implicit dgraph targets") {
      doTestLoadEdges(() =>
        spark
          .read
          .dgraph.edges(dgraph.target)
      )
    }

    it("should load edges in chunks") {
      doTestLoadEdges(() =>
        spark
          .read
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
        spark
          .read
          .format(EdgesSource)
          .load(dgraph.target)
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
    val execution = DgraphExecutorProvider(None)
    val encoder = EdgeEncoder(schema.predicateMap)
    implicit val model: EdgeTableModel = EdgeTableModel(execution, encoder, ChunkSizeDefault)

    it("should load as a single partition") {
      val target = dgraph.target
      val targets = Seq(Target(target))
      val partitions =
        spark
          .read
          .option(PartitionerOption, SingletonPartitionerOption)
          .dgraph.edges(target)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition[_] => Some(p.inputPartition)
          case _ => None
        }
      assert(partitions === Seq(Some(Partition(targets).has(Set(Predicate("director", "uid"), Predicate("starring", "uid"))))))
    }

    it("should load as a predicate partitions") {
      val target = dgraph.target
      val partitions =
        spark
          .read
          .option(PartitionerOption, PredicatePartitionerOption)
          .option(PredicatePartitionerPredicatesOption, "2")
          .dgraph.edges(target)
          .rdd
          .partitions.map {
          case p: DataSourceRDDPartition[_] => Some(p.inputPartition)
          case _ => None
        }

      val expected = Set(
        Some(Partition(Seq(Target(dgraph.target))).has(Set.empty, Set("director")).getAll),
        Some(Partition(Seq(Target(dgraph.target))).has(Set.empty, Set("starring")).getAll)
      )

      assert(partitions.toSet === expected)
    }

    it("should partition data") {
      val target = dgraph.target
      val partitions =
        spark
          .read
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
      spark
        .read
        .options(Map(
          PartitionerOption -> PredicatePartitionerOption,
          PredicatePartitionerPredicatesOption -> "2"
        ))
        .dgraph.edges(dgraph.target)
    lazy val edgesSinglePredicatePerPartition =
      spark
        .read
        .options(Map(
          PartitionerOption -> PredicatePartitionerOption,
          PredicatePartitionerPredicatesOption -> "1"
        ))
        .dgraph.edges(dgraph.target)

    it("should push subject filters") {
      doTestFilterPushDown(
        $"subject" === dgraph.leia,
        Set(SubjectIsIn(Uid(dgraph.leia))),
        expectedDf = expectedEdges.filter(_.getLong(0) == dgraph.leia)
      )

      doTestFilterPushDown(
        $"subject".isin(dgraph.leia),
        Set(SubjectIsIn(Uid(dgraph.leia))),
        expectedDf = expectedEdges.filter(_.getLong(0) == dgraph.leia)
      )

      doTestFilterPushDown(
        $"subject".isin(dgraph.leia, dgraph.luke),
        Set(SubjectIsIn(Uid(dgraph.leia), Uid(dgraph.luke))),
        expectedDf = expectedEdges.filter(r => Set(dgraph.leia, dgraph.luke).contains(r.getLong(0)))
      )
    }

    it("should push predicate filters") {
      doTestFilterPushDown(
        $"predicate" === "director",
        Set(IntersectPredicateNameIsIn("director")),
        expectedDf = expectedEdges.filter(_.getString(1) == "director")
      )

      doTestFilterPushDown(
        $"predicate".isin("director"),
        Set(IntersectPredicateNameIsIn("director")),
        expectedDf = expectedEdges.filter(_.getString(1) == "director")
      )

      doTestFilterPushDown(
        $"predicate".isin("director", "starring"),
        Set(IntersectPredicateNameIsIn("director", "starring")),
        expectedDf = expectedEdges.filter(r => Set("director", "starring").contains(r.getString(1)))
      )
    }

    it("should push object value filters") {
      doTestFilterPushDownDf(
        edges,
        $"objectUid" === dgraph.leia,
        Set(ObjectValueIsIn(dgraph.leia), ObjectTypeIsIn("uid")),
        // With multiple predicates per partition, we cannot filter for object values
        Seq(EqualTo(AttributeReference("objectUid", LongType, nullable = true)(), Literal(dgraph.leia))),
        expectedDs = expectedEdges.filter(_.getLong(2) == dgraph.leia)
      )
      doTestFilterPushDownDf(
        edgesSinglePredicatePerPartition,
        $"objectUid" === dgraph.leia,
        Set(ObjectValueIsIn(dgraph.leia), ObjectTypeIsIn("uid")),
        expectedDs = expectedEdges.filter(_.getLong(2) == dgraph.leia)
      )

      doTestFilterPushDownDf(
        edges,
        $"objectUid".isin(dgraph.leia),
        Set(ObjectValueIsIn(dgraph.leia), ObjectTypeIsIn("uid")),
        // With multiple predicates per partition, we cannot filter for object values
        Seq(EqualTo(AttributeReference("objectUid", LongType, nullable = true)(), Literal(dgraph.leia))),
        expectedDs = expectedEdges.filter(_.getLong(2) == dgraph.leia)
      )
      doTestFilterPushDownDf(
        edgesSinglePredicatePerPartition,
        $"objectUid".isin(dgraph.leia),
        Set(ObjectValueIsIn(dgraph.leia), ObjectTypeIsIn("uid")),
        expectedDs = expectedEdges.filter(_.getLong(2) == dgraph.leia)
      )

      doTestFilterPushDownDf(
        edges,
        $"objectUid".isin(dgraph.leia, dgraph.lucas),
        Set(ObjectValueIsIn(dgraph.leia, dgraph.lucas), ObjectTypeIsIn("uid")),
        // With multiple predicates per partition, we cannot filter for object values
        Seq(In(AttributeReference("objectUid", LongType, nullable = true)(), Seq(Literal(dgraph.leia), Literal(dgraph.lucas)))),
        expectedDs = expectedEdges.filter(r => Set(dgraph.leia, dgraph.lucas).contains(r.getLong(2)))
      )
      doTestFilterPushDownDf(
        edgesSinglePredicatePerPartition,
        $"objectUid".isin(dgraph.leia, dgraph.lucas),
        Set(ObjectValueIsIn(dgraph.leia, dgraph.lucas), ObjectTypeIsIn("uid")),
        expectedDs = expectedEdges.filter(r => Set(dgraph.leia, dgraph.lucas).contains(r.getLong(2)))
      )
    }

    def doTestFilterPushDown(condition: Column, expectedFilters: Set[Filter], expectedUnpushed: Seq[Expression] = Seq.empty, expectedDf: Set[Row]): Unit = {
      doTestFilterPushDownDf(edges, condition, expectedFilters, expectedUnpushed, expectedDf)
    }

    it("should not push projection") {
      doTestProjectionPushDownDf(
        edges,
        Seq($"subject", $"objectUid"),
        None,
        expectedEdges.map(select(0, 2))
      )

      doTestProjectionPushDownDf(
        edges,
        Seq($"subject", $"predicate", $"objectUid"),
        None,
        expectedEdges
      )

      doTestProjectionPushDownDf(
        edges,
        Seq.empty,
        None,
        expectedEdges
      )
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
