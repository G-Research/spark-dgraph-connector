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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector._

class TestEdgeSource extends FunSpec
  with SparkTestSession with DgraphTestCluster
  with FilterPushDownTestHelper {

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
          case p: DataSourceRDDPartition => Some(p.inputPartition)
          case _ => None
        }
      assert(partitions === Seq(Some(Partition(targets, Set(Predicate("director", "uid"), Predicate("starring", "uid")), None, None))))
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
          case p: DataSourceRDDPartition => Some(p.inputPartition)
          case _ => None
        }

      val expected = Set(
        Some(Partition(Seq(Target(cluster.grpc)), Set(Predicate("director", "uid")), None, None)),
        Some(Partition(Seq(Target(cluster.grpc)), Set(Predicate("starring", "uid")), None, None))
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

      // we can only count and retrieve triples, not edges only, and filter for edges in the connector
      // this produces empty partitions: https://github.com/G-Research/spark-dgraph-connector/issues/19
      // so we see a partitioning like (1,2),(3),(),(),() or (),(4),(5),(),(9)
      val uids = Set(sw1, sw2, sw3)
      val expected = allUids.grouped(2).map(p => p.toSet.intersect(uids)).toList
      assert(partitions === expected, s"all uids: $allUids uids with edges: $uids all uids grouped: ${allUids.grouped(2)} expected: $expected")
    }

    lazy val edges =
      spark
        .read
        .options(Map(
          PartitionerOption -> PredicatePartitionerOption,
          PredicatePartitionerPredicatesOption -> "2"
        ))
        .dgraphEdges(cluster.grpc)

    it("should push predicate filters") {
      doTestFilterPushDown($"predicate" === "director", Seq(PredicateNameIsIn("director")), expectedDf = expectedEdges.filter(_.getString(1) == "director"))
      doTestFilterPushDown($"predicate".isin("director"), Seq(PredicateNameIsIn("director")), expectedDf = expectedEdges.filter(_.getString(1) == "director"))
      doTestFilterPushDown($"predicate".isin("director", "starring"), Seq(PredicateNameIsIn("director", "starring")), expectedDf = expectedEdges.filter(r => Set("director", "starring").contains(r.getString(1))))
    }

    it("should push object value filters") {
      doTestFilterPushDown($"objectUid" === leia, Seq(ObjectValueIsIn(leia), ObjectTypeIsIn("uid")), expectedDf = expectedEdges.filter(_.getLong(2) == leia))
      doTestFilterPushDown($"objectUid".isin(leia), Seq(ObjectValueIsIn(leia), ObjectTypeIsIn("uid")), expectedDf = expectedEdges.filter(_.getLong(2) == leia))
      doTestFilterPushDown($"objectUid".isin(leia, lucas), Seq(ObjectValueIsIn(leia, lucas), ObjectTypeIsIn("uid")), expectedDf = expectedEdges.filter(r => Set(leia, lucas).contains(r.getLong(2))))
    }

    def doTestFilterPushDown(condition: Column, expectedFilters: Seq[Filter], expectedUnpushed: Seq[Expression] = Seq.empty, expectedDf: Set[Row]): Unit = {
      doTestFilterPushDownDf(edges, condition, expectedFilters, expectedUnpushed, expectedDf)
    }

  }

}
