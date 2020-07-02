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

package uk.co.gresearch.spark.dgraph.connector.partitioner

import java.util.UUID

import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.executor.JsonGraphQlExecutor
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Json, Partition, Predicate, Schema, Target, UidRange}

class TestUidRangePartitioner extends FunSpec {

  describe("UidRangePartitioner") {

    val schema = Schema(Set(Predicate(s"pred1", s"type1"), Predicate(s"pred2", s"type2")))
    val clusterState = ClusterState(
      Map(
        "1" -> Set(Target("host1:9080"), Target("host2:9080")),
        "2" -> Set(Target("host3:9080"))
      ),
      Map(
        "1" -> Set("pred1"),
        "2" -> Set("pred2")
      ),
      10000,
      UUID.randomUUID()
    )

    val singleton = SingletonPartitioner(Seq(Target("host1:9080"), Target("host2:9080"), Target("host3:9080")))
    val group = GroupPartitioner(schema, clusterState)
    val alpha = AlphaPartitioner(schema, clusterState, 1)
    val pred = PredicatePartitioner(schema, clusterState, 1)

    val testPartitioners =
      Seq(
        (singleton, "singleton"),
        (group, "group"),
        (alpha, "alpha"),
        (pred, "predicates"),
      )

    val testSizes = Seq(2000, 5000)

    testPartitioners.foreach { case (partitioner, label) =>
      testSizes.foreach { size =>

        it(s"should decorate $label partitioner with ${size} uids per partition") {
          val uidPartitioner = UidRangePartitioner(partitioner, size, UidCardinalityEstimator.forMaxLeaseId(clusterState.maxLeaseId))
          val partitions = partitioner.getPartitions
          val uidPartitions = uidPartitioner.getPartitions

          val ranges = (0 until (10000 / size)).map(idx => UidRange(idx * size, size))
          assert(uidPartitions.length === partitions.length * ranges.length)
          val expectedPartitions = partitions.flatMap( partition =>
            ranges.zipWithIndex.map { case (range, idx) =>
              Partition(partition.targets.rotateLeft(idx), partition.predicates, Some(range))
            }
          )

          assert(uidPartitions === expectedPartitions)
        }

      }

      it(s"should decorate $label partitioner with uid count estimator") {
        val executor = new JsonGraphQlExecutor {
          override def query(query: connector.GraphQl): connector.Json = Json("""{ "result": [ { "count": 8 } ] }""")
        }
        val uidPartitioner = UidRangePartitioner(partitioner, 5, UidCardinalityEstimator.forExecutor(executor))
        val partitions = partitioner.getPartitions
        val uidPartitions = uidPartitioner.getPartitions

        val ranges = Seq(UidRange(0, 5), UidRange(5, 5))
        assert(uidPartitions.length === partitions.length * ranges.length)
        val expectedPartitions = partitions.flatMap( partition =>
          ranges.zipWithIndex.map { case (range, idx) =>
            Partition(partition.targets.rotateLeft(idx), partition.predicates, Some(range))
          }
        )

        assert(uidPartitions === expectedPartitions)
      }

    }

    testPartitioners.foreach{ case (partitioner, label) =>

      it(s"should noop $label partitioner with too large uidsPerPartition") {
        val uidPartitioner = UidRangePartitioner(partitioner, clusterState.maxLeaseId.toInt, UidCardinalityEstimator.forMaxLeaseId(clusterState.maxLeaseId))
        assert(uidPartitioner.getPartitions === partitioner.getPartitions)
      }

    }

    it("should fail on null partitioner") {
      assertThrows[IllegalArgumentException] {
        UidRangePartitioner(null, 1, UidCardinalityEstimator.forMaxLeaseId(1000))
      }
    }

    it("should fail with integer overflow partition size") {
      assertThrows[IllegalArgumentException] {
        UidRangePartitioner(singleton, 1, UidCardinalityEstimator.forMaxLeaseId(Long.MaxValue)).getPartitions
      }
    }

    it("should fail on decorating uid partitioner") {
      val uidPartitioner = UidRangePartitioner(singleton, 2, UidCardinalityEstimator.forMaxLeaseId(1000))
      assertThrows[IllegalArgumentException] {
        UidRangePartitioner(uidPartitioner, 1, UidCardinalityEstimator.forMaxLeaseId(1000))
      }
    }

  }

}
