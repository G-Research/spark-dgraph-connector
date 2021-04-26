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
  with ConnectorSparkTestSession with DgraphTestCluster {

  import spark.implicits._

  describe("PerfSource") {

    it("should provide performance statistic") {
      val perfs =
        reader
          .format("uk.co.gresearch.spark.dgraph.connector.sources.PerfSource")
          .options(Map(
            PredicatePartitionerPredicatesOption -> "4",
            UidRangePartitionerUidsPerPartOption -> "4",
            ChunkSizeOption -> "2",
            MaxLeaseIdEstimatorIdOption -> dgraph.highestUid.toString
          ))
          .load(dgraph.target)
          .as[Perf]
          .collect()
          .sortBy(_.sparkPartitionId)

      assert(perfs.length >= 4 && perfs.length < 12)  // rough limits
      assert(perfs.forall(_.partitionTargets === Seq(dgraph.target)))
      assert(perfs.forall(p =>
        p.partitionPredicates.exists(_ === Seq("release_date", "dgraph.type", "name", "starring")) ||
          p.partitionPredicates.exists(_ === Seq("revenue", "running_time", "title", "director"))
      ))
      assert(perfs.forall(_.partitionFirstUid.exists(_ <= dgraph.highestUid)))
      assert(perfs.forall(_.partitionUidLength.exists(_ === 4)))

      assert(perfs.forall(p => p.chunkAfterUid >= 0 && p.chunkAfterUid < dgraph.highestUid))
      assert(perfs.forall(p => p.chunkUidLength > 0 && p.chunkUidLength <= 2))
      assert(perfs.forall(p => p.chunkBytes > 0 && p.chunkBytes < 10240))
      assert(perfs.forall(p => p.chunkNodes > 0 && p.chunkNodes <= 2))

      assert(perfs.forall(_.sparkStageId >= 0))
      assert(perfs.forall(_.sparkStageAttemptNumber >= 0))
      assert(perfs.forall(_.sparkPartitionId >= 0))
      assert(perfs.forall(_.sparkPartitionId < 20))
      assert(perfs.forall(_.sparkTaskAttemptId >= 0))

      assert(perfs.forall(_.dgraphAssignTimestampNanos.exists(_ > 0)))
      assert(perfs.forall(_.dgraphParsingNanos.exists(_ > 0)))
      assert(perfs.forall(_.dgraphProcessingNanos.exists(_ > 0)))
      assert(perfs.forall(_.dgraphEncodingNanos.exists(_ > 0)))
      assert(perfs.forall(_.dgraphTotalNanos.exists(_ > 0)))

      assert(perfs.forall(_.connectorWireNanos > 0))
      assert(perfs.forall(_.connectorDecodeNanos > 0))
    }

  }

}
