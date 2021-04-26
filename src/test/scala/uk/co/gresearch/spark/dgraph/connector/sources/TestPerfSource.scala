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

    it("should load via paths") {
//      val perfs =
      val df =
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
      df.show(false)
      val perfs = df
          .collect()
          .sortBy(_.sparkPartitionId)

      assert(perfs.forall(_.chunkBytes > 0))
      assert(perfs.forall(_.chunkBytes < 1024))

      assert(perfs.forall(_.sparkStageId >= 0))
      assert(perfs.forall(_.sparkStageAttemptNumber >= 0))
      assert(perfs.forall(_.sparkPartitionId >= 0))
      assert(perfs.forall(_.sparkPartitionId <= 5))
      assert(perfs.forall(_.sparkTaskAttemptId >= 0))

      assert(perfs.forall(_.dgraphAssignTimestampNanos.exists(_ > 0)))
      assert(perfs.forall(_.dgraphParsingNanos.exists(_ > 0)))
      assert(perfs.forall(_.dgraphProcessingNanos.exists(_ > 0)))
      assert(perfs.forall(_.dgraphEncodingNanos.exists(_ > 0)))
      assert(perfs.forall(_.dgraphTotalNanos.exists(_ > 0)))

      assert(perfs.forall(_.connectorWireNanos > 0))
      assert(perfs.forall(_.connectorDecodeNanos > 0))

      // spark and dgraph numbers are not deterministic
      val actual = perfs.map(_.copy(chunkBytes = 0, sparkStageId = 0, sparkStageAttemptNumber = 0, sparkAttemptNumber = 0, sparkTaskAttemptId = 0, dgraphAssignTimestampNanos = None, dgraphParsingNanos = None, dgraphProcessingNanos = None, dgraphEncodingNanos = None, dgraphTotalNanos = None, connectorWireNanos = 0, connectorDecodeNanos = 0))
      val expected = Array(
        Perf(List(dgraph.target), Some(List("release_date", "dgraph.type", "name", "starring")), Some(1), Some(4), 0x0, 2, 0, 2, 0, 0, 0, 0, 0, None, None, None, None, None, 0, 0),
        Perf(List(dgraph.target), Some(List("release_date", "dgraph.type", "name", "starring")), Some(1), Some(4), 0x3, 1, 0, 1, 0, 0, 0, 0, 0, None, None, None, None, None, 0, 0),
        Perf(List(dgraph.target), Some(List("release_date", "dgraph.type", "name", "starring")), Some(5), Some(4), 0x4, 2, 0, 2, 0, 0, 1, 0, 0, None, None, None, None, None, 0, 0),
        Perf(List(dgraph.target), Some(List("release_date", "dgraph.type", "name", "starring")), Some(5), Some(4), 0x6, 2, 0, 2, 0, 0, 1, 0, 0, None, None, None, None, None, 0, 0),
        Perf(List(dgraph.target), Some(List("release_date", "dgraph.type", "name", "starring")), Some(9), Some(4), 0x8, 2, 0, 2, 0, 0, 2, 0, 0, None, None, None, None, None, 0, 0),
        Perf(List(dgraph.target), Some(List("release_date", "dgraph.type", "name", "starring")), Some(9), Some(4), 0xa, 2, 0, 2, 0, 0, 2, 0, 0, None, None, None, None, None, 0, 0),
        Perf(List(dgraph.target), Some(List("revenue", "running_time", "title", "director")), Some(1), Some(4), 0x0, 2, 0, 2, 0, 0, 3, 0, 0, None, None, None, None, None, 0, 0),
        Perf(List(dgraph.target), Some(List("revenue", "running_time", "title", "director")), Some(9), Some(4), 0x8, 2, 0, 2, 0, 0, 5, 0, 0, None, None, None, None, None, 0, 0)
      )
      assert(actual === expected)
    }

  }

}
