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

package uk.co.gresearch.spark.dgraph.connector.partitioner

import java.util.UUID

import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Partition, Predicate, Schema, Target}

class TestAlphaPartitioner extends FunSpec {

  describe("AlphaPartitioner") {

    val schema = Schema((1 to 7).map(i => Predicate(s"pred$i", s"type$i", s"type$i")).toSet)
    val clusterState = ClusterState(
      Map(
        "1" -> Set(Target("host1:9080")),
        "2" -> Set(Target("host2:9080"), Target("host3:9080")),
        "3" -> Set(Target("host4:9080"), Target("host5:9080")),
        "4" -> Set(Target("host6:9080"))
      ),
      Map(
        "1" -> Set.empty,
        "2" -> Set("pred1", "pred2", "pred3", "pred4"),
        "3" -> Set("pred5"),
        "4" -> Set("pred6", "pred7")
      ),
      10000,
      UUID.randomUUID()
    )

    it("should partition with 1 partition per alpha") {
      val partitioner = AlphaPartitioner(schema, clusterState, 1)
      val partitions = partitioner.getPartitions

      assert(partitions.toSet === Set(
        // predicates are shuffled within group and alpha, targets rotate within group, empty group does not get a partition
        Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set("pred1", "pred2"), Set.empty).getAll,
        Partition(Seq(Target("host3:9080"), Target("host2:9080"))).has(Set("pred3", "pred4"), Set.empty).getAll,

        Partition(Seq(Target("host4:9080"), Target("host5:9080"))).has(Set("pred5"), Set.empty).getAll,

        Partition(Seq(Target("host6:9080"))).has(Set("pred7", "pred6"), Set.empty).getAll
      ))
    }

    Seq(2, 3, 7).foreach(partsPerAlpha =>
      it(s"should partition with $partsPerAlpha partitions per alpha") {
        val partitioner = AlphaPartitioner(schema, clusterState, partsPerAlpha)
        val partitions = partitioner.getPartitions

        assert(partitions.toSet === Set(
          // predicates are shuffled within group and alpha, targets rotate within group, empty group does not get a partition
          Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set("pred1"), Set.empty).getAll,
          Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set("pred2"), Set.empty).getAll,
          Partition(Seq(Target("host3:9080"), Target("host2:9080"))).has(Set("pred3"), Set.empty).getAll,
          Partition(Seq(Target("host3:9080"), Target("host2:9080"))).has(Set("pred4"), Set.empty).getAll,

          Partition(Seq(Target("host4:9080"), Target("host5:9080"))).has(Set("pred5"), Set.empty).getAll,

          Partition(Seq(Target("host6:9080"))).has(Set("pred6"), Set.empty).getAll,
          Partition(Seq(Target("host6:9080"))).has(Set("pred7"), Set.empty).getAll
        ))
      }
    )

    it("should fail with negative or zero partsPerAlpha") {
      assertThrows[IllegalArgumentException]{ AlphaPartitioner(schema, clusterState, -1) }
      assertThrows[IllegalArgumentException]{ AlphaPartitioner(schema, clusterState, 0) }
    }

  }

}
