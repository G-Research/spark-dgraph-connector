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

import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector._

class TestPerfSource extends AnyFunSpec
  with SparkTestSession with DgraphTestCluster {

  import spark.implicits._

  describe("PerfSource") {

    it("should load via paths") {
      val perfs =
        spark
          .read
          .format("uk.co.gresearch.spark.dgraph.connector.sources.PerfSource")
          .options(Map(
            PredicatePartitionerPredicatesOption -> "2",
            UidRangePartitionerUidsPerPartOption -> "2"
          ))
          .load(dgraph.target)
          .as[Perf]
          .collect()
          .sortBy(_.sparkPartitionId)

      // Example:
      // Perf(Seq("localhost:9080"), Some(Seq("release_date", "revenue")), Some(0), Some(2), 0, 0, 0, 0, 0, Some(450097), Some(95947), Some(309825), Some(20358), Some(945510)),
      // Perf(Seq("localhost:9080"), Some(Seq("release_date", "revenue")), Some(2), Some(2), 0, 0, 1, 0, 1, Some(454868), Some(78280), Some(321795), Some(21096), Some(944941)),
      // Perf(Seq("localhost:9080"), Some(Seq("dgraph.graphql.schema", "starring")), Some(0), Some(2), 0, 0, 2, 0, 2, Some(401568), Some(72688), Some(197814), Some(10291), Some(741997)),
      // Perf(Seq("localhost:9080"), Some(Seq("dgraph.graphql.schema", "starring")), Some(2), Some(2), 0, 0, 3, 0, 3, Some(345644), Some(112154), Some(231255), Some(8781), Some(754845)),
      // Perf(Seq("localhost:9080"), Some(Seq("director", "running_time")), Some(0), Some(2), 0, 0, 4, 0, 4, Some(352526), Some(75411), Some(283403), Some(9663), Some(781171)),
      // Perf(Seq("localhost:9080"), Some(Seq("director", "running_time")), Some(2), Some(2), 0, 0, 5, 0, 5, Some(315593), Some(66102), Some(256086), Some(10080), Some(703224)),
      // Perf(Seq("localhost:9080"), Some(Seq("dgraph.type", "name")), Some(0), Some(2), 0, 0, 6, 0, 6, Some(381511), Some(71763), Some(216050), Some(11367), Some(731836)),
      // Perf(Seq("localhost:9080"), Some(Seq("dgraph.type", "name")), Some(2), Some(2), 0, 0, 7, 0, 7, Some(330556), Some(68906), Some(249247), Some(13140), Some(721444)),
      // Perf(Seq("localhost:9080"), Some(Seq("dgraph.type", "name")), Some(4), Some(2), 0, 0, 8, 0, 8, Some(393403), Some(92074), Some(216785), Some(10273), Some(779874)),
      // Perf(Seq("localhost:9080"), Some(Seq("dgraph.type", "name")), Some(6), Some(2), 0, 0, 9, 0, 9, Some(394063), Some(86764), Some(238309), Some(10604), Some(797032)),
      // Perf(Seq("localhost:9080"), Some(Seq("dgraph.type", "name")), Some(8), Some(2), 0, 0, 10, 0, 10, Some(365052), Some(68823), Some(294403), Some(15287), Some(807023)),

      assert(perfs.length === 11)
      assert(perfs.forall(_.partitionTargets == Seq(dgraph.target)))
      assert(perfs.map(_.partitionPredicates).distinct === Seq(
        Some(Seq("release_date", "revenue")),
        Some(Seq("dgraph.graphql.schema", "starring")),
        Some(Seq("director", "running_time")),
        Some(Seq("dgraph.type", "name")),
      ))
      assert(perfs.map(p => (p.partitionPredicates, p.partitionUidsFirst)).groupBy(_._1.get).mapValues(_.map(_._2.get).toSeq) === Map(
        Seq("release_date", "revenue") -> Seq(0, 2),
        Seq("dgraph.graphql.schema", "starring") -> Seq(0, 2),
        Seq("director", "running_time") -> Seq(0, 2),
        Seq("dgraph.type", "name") -> Seq(0, 2, 4, 6, 8),
      ))
      assert(perfs.forall(_.partitionUidsLength.contains(2)))

      assert(perfs.forall(_.sparkStageId == 0))
      assert(perfs.forall(_.sparkStageAttemptNumber == 0))
      assert(perfs.zipWithIndex.forall { case (perf, idx) => perf.sparkPartitionId == idx })
      assert(perfs.forall(_.sparkAttemptNumber == 0))
      assert(perfs.zipWithIndex.forall { case (perf, idx) => perf.sparkTaskAttemptId == idx })

      assert(perfs.forall(_.dgraphAssignTimestamp.isDefined))
      assert(perfs.forall(_.dgraphParsing.isDefined))
      assert(perfs.forall(_.dgraphProcessing.isDefined))
      assert(perfs.forall(_.dgraphEncoding.isDefined))
      assert(perfs.forall(_.dgraphAssignTimestamp.isDefined))
    }

  }

}
