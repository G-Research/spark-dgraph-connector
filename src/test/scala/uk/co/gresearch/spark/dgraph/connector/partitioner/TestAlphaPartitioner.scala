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
import uk.co.gresearch.spark.dgraph.connector.encoder.TypedTripleEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.DgraphExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.model.TripleTableModel
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Partition, Predicate, Schema, Target}

class TestAlphaPartitioner extends FunSpec {

  describe("AlphaPartitioner") {

    val schema = Schema((1 to 7).map(i => Predicate(s"pred$i", s"type$i")).toSet)
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
    val execution = DgraphExecutorProvider()
    val encoder = TypedTripleEncoder(schema.predicateMap)
    val model = TripleTableModel(execution, encoder)

    it("should partition with 1 partition per alpha") {
      val partitioner = AlphaPartitioner(schema, clusterState, 1)
      val partitions = partitioner.getPartitions(model)

      assert(partitions.length === 4)
      assert(partitions.toSet === Set(
        // predicates are shuffled within group and alpha, targets rotate within group, empty group does not get a partition
        Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred1", "type1"), Predicate("pred2", "type2"))), None, model),
        Partition(Seq(Target("host3:9080"), Target("host2:9080")), Some(Set(Predicate("pred3", "type3"), Predicate("pred4", "type4"))), None, model),

        Partition(Seq(Target("host4:9080"), Target("host5:9080")), Some(Set(Predicate("pred5", "type5"))), None, model),

        Partition(Seq(Target("host6:9080")), Some(Set(Predicate("pred7", "type7"), Predicate("pred6", "type6"))), None, model)
      ))
    }

    Seq(2, 3, 7).foreach(partsPerAlpha =>
      it(s"should partition with $partsPerAlpha partitions per alpha") {
        val partitioner = AlphaPartitioner(schema, clusterState, 2)
        val partitions = partitioner.getPartitions(model)

        assert(partitions.length === 7)
        assert(partitions.toSet === Set(
          // predicates are shuffled within group and alpha, targets rotate within group, empty group does not get a partition
          Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred1", "type1"))), None, model),
          Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred2", "type2"))), None, model),
          Partition(Seq(Target("host3:9080"), Target("host2:9080")), Some(Set(Predicate("pred3", "type3"))), None, model),
          Partition(Seq(Target("host3:9080"), Target("host2:9080")), Some(Set(Predicate("pred4", "type4"))), None, model),

          Partition(Seq(Target("host4:9080"), Target("host5:9080")), Some(Set(Predicate("pred5", "type5"))), None, model),

          Partition(Seq(Target("host6:9080")), Some(Set(Predicate("pred6", "type6"))), None, model),
          Partition(Seq(Target("host6:9080")), Some(Set(Predicate("pred7", "type7"))), None, model)
        ))
      }
    )

    it("should fail with negative or zero partsPerAlpha") {
      assertThrows[IllegalArgumentException]{ AlphaPartitioner(schema, clusterState, -1) }
      assertThrows[IllegalArgumentException]{ AlphaPartitioner(schema, clusterState, 0) }
    }

  }

}
