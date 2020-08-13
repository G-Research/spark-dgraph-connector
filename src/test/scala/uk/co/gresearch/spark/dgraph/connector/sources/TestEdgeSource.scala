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

import io.dgraph.DgraphProto.TxnContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.encoder.EdgeEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.DgraphExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.model.EdgeTableModel

class TestEdgeSource extends FunSpec
  with SparkTestSession with DgraphTestCluster
  with FilterPushdownTestHelper
  with ProjectionPushDownTestHelper {

  import spark.implicits._

  describe("EdgeDataSource") {

    lazy val expectedEdges = Set(
      Row(sw1, "director", lucas),
      Row(sw1, "starring", leia),
      Row(sw1, "starring", luke),
      Row(sw1, "starring", han),
      Row(sw2, "director", irvin),
      Row(sw2, "starring", leia),
      Row(sw2, "starring", luke),
      Row(sw2, "starring", han),
      Row(sw3, "director", richard),
      Row(sw3, "starring", leia),
      Row(sw3, "starring", luke),
      Row(sw3, "starring", han),
    )

    def doTestLoadEdges(load: () => DataFrame): Unit = {
      val edges = load().collect().toSet
      assert(edges === expectedEdges)
    }

    it("should load edges via path") {
      doTestLoadEdges(() =>
        spark
          .read
          .format(EdgesSource)
          .load(cluster.grpc)
      )
    }

    it("should load edges via paths") {
      doTestLoadEdges(() =>
        spark
          .read
          .format(EdgesSource)
          .load(cluster.grpc, cluster.grpcLocalIp)
      )
    }

    it("should load edges via target option") {
      doTestLoadEdges(() =>
        spark
          .read
          .format(EdgesSource)
          .option(TargetOption, cluster.grpc)
          .load()
      )
    }

    it("should load edges via targets option") {
      doTestLoadEdges(() =>
        spark
          .read
          .format(EdgesSource)
          .option(TargetsOption, s"""["${cluster.grpc}","${cluster.grpcLocalIp}"]""")
          .load()
      )
    }

    it("should load edges via implicit dgraph target") {
      doTestLoadEdges(() =>
        spark
          .read
          .dgraphEdges(cluster.grpc)
      )
    }

    it("should load edges via implicit dgraph targets") {
      doTestLoadEdges(() =>
        spark
          .read
          .dgraphEdges(cluster.grpc, cluster.grpcLocalIp)
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
          .dgraphEdges(cluster.grpc, cluster.grpcLocalIp)
      )
    }

    it("should encode Edge") {
      val rows =
        spark
          .read
          .format(EdgesSource)
          .load(cluster.grpc)
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
    val transaction: Transaction = Transaction(TxnContext.newBuilder().build())
    val execution = DgraphExecutorProvider(transaction)
    val encoder = EdgeEncoder(schema.predicateMap)
    implicit val model: EdgeTableModel = EdgeTableModel(execution, encoder, ChunkSizeDefault)

    it("should load as a single partition") {
      val target = cluster.grpc
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
      assert(partitions === Seq(Some(Partition(targets).has(Set(Predicate("director", "uid"), Predicate("starring", "uid"))))))
    }

    it("should load as a predicate partitions") {
      val target = cluster.grpc
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

      val expected = Set(
        Some(Partition(Seq(Target(cluster.grpc))).has(Set.empty, Set("director")).getAll),
        Some(Partition(Seq(Target(cluster.grpc))).has(Set.empty, Set("starring")).getAll)
      )

      assert(partitions.toSet === expected)
    }

    it("should partition data") {
      val target = cluster.grpc
      val partitions =
        spark
          .read
          .options(Map(
            PartitionerOption -> UidRangePartitionerOption,
            UidRangePartitionerUidsPerPartOption -> "2",
            UidRangePartitionerEstimatorOption -> MaxLeaseIdEstimatorOption,
            MaxLeaseIdEstimatorIdOption -> highestUid.toString
          ))
          .dgraphEdges(target)
          .mapPartitions(part => Iterator(part.map(_.getLong(0)).toSet))
          .collect()

      // we retrieve partitions in chunks of 2 uids, if there are uids allocated but unused then we get partitions with less than 2 uids
      val uids = Set(sw1, sw2, sw3).map(_.toInt)
      val expected = (1 to highestUid.toInt).grouped(2).map(_.toSet intersect uids).toSeq
      assert(partitions === expected)
    }

    lazy val edges =
      spark
        .read
        .options(Map(
          PartitionerOption -> PredicatePartitionerOption,
          PredicatePartitionerPredicatesOption -> "2"
        ))
        .dgraphEdges(cluster.grpc)

    it("should push subject filters") {
      doTestFilterPushDown(
        $"subject" === leia,
        Set(SubjectIsIn(Uid(leia))),
        expectedDf = expectedEdges.filter(_.getLong(0) == leia)
      )

      doTestFilterPushDown(
        $"subject".isin(leia),
        Set(SubjectIsIn(Uid(leia))),
        expectedDf = expectedEdges.filter(_.getLong(0) == leia)
      )

      doTestFilterPushDown(
        $"subject".isin(leia, luke),
        Set(SubjectIsIn(Uid(leia), Uid(luke))),
        expectedDf = expectedEdges.filter(r => Set(leia, luke).contains(r.getLong(0)))
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
      doTestFilterPushDown(
        $"objectUid" === leia,
        Set(ObjectValueIsIn(leia), ObjectTypeIsIn("uid")),
        expectedDf = expectedEdges.filter(_.getLong(2) == leia)
      )

      doTestFilterPushDown(
        $"objectUid".isin(leia),
        Set(ObjectValueIsIn(leia), ObjectTypeIsIn("uid")),
        expectedDf = expectedEdges.filter(_.getLong(2) == leia)
      )

      doTestFilterPushDown(
        $"objectUid".isin(leia, lucas),
        Set(ObjectValueIsIn(leia, lucas), ObjectTypeIsIn("uid")),
        expectedDf = expectedEdges.filter(r => Set(leia, lucas).contains(r.getLong(2)))
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
