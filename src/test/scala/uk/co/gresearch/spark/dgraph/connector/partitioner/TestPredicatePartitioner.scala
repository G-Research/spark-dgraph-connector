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
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.encoder.TypedTripleEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.DgraphExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.model.TripleTableModel

class TestPredicatePartitioner extends FunSpec {

  describe("PredicatePartitioner") {

    val P = 10
    assert(P > 2)
    val predicates = (1 to P).map(i => Predicate(s"pred$i", s"type$i", s"type$i")).toSet

    ((1 to P) ++ Seq(P + 1, P + 2, P + 3)).foreach { N =>

      it(s"should shard $P predicates into $N shards") {
        val shards = PredicatePartitioner.shard(predicates, N)
        assert(shards.length <= math.min(N, P))
        assert(shards.map(_.size).sum === predicates.size)
        assert(shards.flatten.toSet === predicates)
      }

    }

    ((1 to P) ++ Seq(P + 1, P + 2, P + 3)).foreach { N =>

      it(s"should partition $P predicates into $N partitions") {
        val partitions = PredicatePartitioner.partition(predicates, N)
        println(partitions.map(_.map(_.predicateName)))
        assert(partitions.length === math.min(N, P))
        (0 until math.min(P % N, P)).foreach(p =>
          assert(partitions(p).size === P / N + 1, s"the ${p + 1}-th partition should have size ${P / N + 1}")
        )
        ((P % N) until math.min(N, P)).foreach(p =>
          assert(partitions(p).size === P / N, s"the ${p + 1}-th partition should have size ${P / N}")
        )
        assert(partitions.map(_.size).sum === predicates.size)
        assert(partitions.flatten.toSet === predicates)
      }

    }

    val schema = Schema((1 to 6).map(i => Predicate(s"pred$i", s"type$i", s"type$i")).toSet)
    val clusterState = ClusterState(
      Map(
        "1" -> Set(Target("host1:9080")),
        "2" -> Set(Target("host2:9080"), Target("host3:9080")),
        "3" -> Set(Target("host4:9080"), Target("host5:9080")),
        "4" -> Set(Target("host6:9080"))
      ),
      Map(
        "1" -> Set.empty,
        "2" -> Set("pred1", "pred2", "pred3"),
        "3" -> Set("pred4", "pred5"),
        "4" -> Set("pred6")
      ),
      10000,
      UUID.randomUUID()
    )
    val execution = DgraphExecutorProvider()
    val encoder = TypedTripleEncoder(schema.predicateMap)
    val model = TripleTableModel(execution, encoder, ChunkSizeDefault)

    it("should partition with 1 predicates per partition") {
      val partitioner = PredicatePartitioner(schema, clusterState, 1)
      val partitions = partitioner.getPartitions(model)

      assert(partitions.toSet === Set(
        // predicates are shuffled within group, targets rotate within group, empty group does not get a partition
        Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred1", "type1", "type1"))), None, None, model),
        Partition(Seq(Target("host3:9080"), Target("host2:9080")), Some(Set(Predicate("pred3", "type3", "type3"))), None, None, model),
        Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred2", "type2", "type2"))), None, None, model),

        Partition(Seq(Target("host4:9080"), Target("host5:9080")), Some(Set(Predicate("pred5", "type5", "type5"))), None, None, model),
        Partition(Seq(Target("host5:9080"), Target("host4:9080")), Some(Set(Predicate("pred4", "type4", "type4"))), None, None, model),

        Partition(Seq(Target("host6:9080")), Some(Set(Predicate("pred6", "type6", "type6"))), None, None, model),
      ))
    }

    it("should partition with 2 predicates per partition") {
      val partitioner = PredicatePartitioner(schema, clusterState, 2)
      val partitions = partitioner.getPartitions(model)

      assert(partitions.toSet === Set(
        // predicates are shuffled within group, targets rotate within group, empty group does not get a partition
        Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred1", "type1", "type1"), Predicate("pred2", "type2", "type2"))), None, None, model),
        Partition(Seq(Target("host3:9080"), Target("host2:9080")), Some(Set(Predicate("pred3", "type3", "type3"))), None, None, model),

        Partition(Seq(Target("host4:9080"), Target("host5:9080")), Some(Set(Predicate("pred5", "type5", "type5"), Predicate("pred4", "type4", "type4"))), None, None, model),

        Partition(Seq(Target("host6:9080")), Some(Set(Predicate("pred6", "type6", "type6"))), None, None, model)
      ))
    }

    Seq(3, 4, 7).foreach(predsPerPart =>
      it(s"should partition with $predsPerPart predicates per partition") {
        val partitioner = PredicatePartitioner(schema, clusterState, predsPerPart)
        val partitions = partitioner.getPartitions(model)

        assert(partitions === Seq(
          // predicates are shuffled within group, targets are not rotated since there is only the first partition per group, empty group does not get a partition
          Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred1", "type1", "type1"), Predicate("pred3", "type3", "type3"), Predicate("pred2", "type2", "type2"))), None, None, model),

          Partition(Seq(Target("host4:9080"), Target("host5:9080")), Some(Set(Predicate("pred5", "type5", "type5"), Predicate("pred4", "type4", "type4"))), None, None, model),

          Partition(Seq(Target("host6:9080")), Some(Set(Predicate("pred6", "type6", "type6"))), None, None, model)
        ))
      }
    )

    it("should fail with negative or zero predsPerPart") {
      assertThrows[IllegalArgumentException] {
        PredicatePartitioner(schema, clusterState, -1)
      }
      assertThrows[IllegalArgumentException] {
        PredicatePartitioner(schema, clusterState, 0)
      }
    }

    it("should apply PredicateNameIsIn filter") {
      val partitioner = PredicatePartitioner(schema, clusterState, 5)
      val partitions1 = partitioner.withFilters(Filters.fromPromised(PredicateNameIsIn("pred3"))).getPartitions(model)
      assert(partitions1 === Seq(Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred3", "type3", "type3"))), None, None, model)))

      val partitions2 = partitioner.withFilters(Filters.fromPromised(PredicateNameIsIn("pred2", "pred3", "pred4"))).getPartitions(model)
      assert(partitions2 === Seq(
        Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred2", "type2", "type2"), Predicate("pred3", "type3", "type3"))), None, None, model),
        Partition(Seq(Target("host4:9080"), Target("host5:9080")), Some(Set(Predicate("pred4", "type4", "type4"))), None, None, model)
      ))
    }

    it("should apply ObjectTypeIsIn filter") {
      val partitioner = PredicatePartitioner(schema, clusterState, 5)
      val partitions1 = partitioner.withFilters(Filters.fromPromised(ObjectTypeIsIn("type3"))).getPartitions(model)
      assert(partitions1 === Seq(Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred3", "type3", "type3"))), None, None, model)))

      val partitions2 = partitioner.withFilters(Filters.fromPromised(ObjectTypeIsIn("type2", "type3", "type4"))).getPartitions(model)
      assert(partitions2 === Seq(
        Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred2", "type2", "type2"), Predicate("pred3", "type3", "type3"))), None, None, model),
        Partition(Seq(Target("host4:9080"), Target("host5:9080")), Some(Set(Predicate("pred4", "type4", "type4"))), None, None, model)
      ))
    }

    it("should apply ObjectValueIsIn with PredicateNameIsIn filter") {
      val partitioner = PredicatePartitioner(schema, clusterState, 5)
      val partitions1 = partitioner.withFilters(Filters.fromPromised(PredicateNameIsIn("pred3"), ObjectValueIsIn("value"))).getPartitions(model)
      assert(partitions1 === Seq(Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred3", "type3", "type3"))), None, Some(Map("pred3" -> Set("value"))), model)))

      val valuesGroup2: Map[String, Set[Any]] = Map(
        "pred2" -> Set("value1", "value2"),
        "pred3" -> Set("value1", "value2")
      )
      val valuesGroup3: Map[String, Set[Any]] = Map(
        "pred4" -> Set("value1", "value2")
      )
      val partitions2 = partitioner.withFilters(Filters.fromPromised(PredicateNameIsIn("pred2", "pred3", "pred4"), ObjectValueIsIn("value1", "value2"))).getPartitions(model)
      assert(partitions2 === Seq(
        Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred2", "type2", "type2"), Predicate("pred3", "type3", "type3"))), None, Some(valuesGroup2), model),
        Partition(Seq(Target("host4:9080"), Target("host5:9080")), Some(Set(Predicate("pred4", "type4", "type4"))), None, Some(valuesGroup3), model)
      ))
    }

    it("should apply ObjectValueIsIn with ObjectTypeIsIn filter") {
      val partitioner = PredicatePartitioner(schema, clusterState, 5)
      val partitions1 = partitioner.withFilters(Filters.fromPromised(ObjectTypeIsIn("type3"), ObjectValueIsIn("value"))).getPartitions(model)
      assert(partitions1 === Seq(Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred3", "type3", "type3"))), None, Some(Map("pred3" -> Set("value"))), model)))

      val valuesGroup2: Map[String, Set[Any]] = Map(
        "pred2" -> Set("value1", "value2"),
        "pred3" -> Set("value1", "value2")
      )
      val valuesGroup3: Map[String, Set[Any]] = Map(
        "pred4" -> Set("value1", "value2")
      )
      val partitions2 = partitioner.withFilters(Filters.fromPromised(ObjectTypeIsIn("type2", "type3", "type4"), ObjectValueIsIn("value1", "value2"))).getPartitions(model)
      assert(partitions2 === Seq(
        Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred2", "type2", "type2"), Predicate("pred3", "type3", "type3"))), None, Some(valuesGroup2), model),
        Partition(Seq(Target("host4:9080"), Target("host5:9080")), Some(Set(Predicate("pred4", "type4", "type4"))), None, Some(valuesGroup3), model)
      ))
      partitions2.foreach(p => println(p.query.forPropertiesAndEdges(None).string))
    }

    it("should not apply ObjectValueIsIn filter only") {
      val partitioner = PredicatePartitioner(schema, clusterState, 5)
      val partitions = partitioner.withFilters(Filters.fromOptional(ObjectValueIsIn("value"))).getPartitions(model)

      // same as in s"should partition with $predsPerPart predicates per partition" above
      assert(partitions === Seq(
        // predicates are shuffled within group, targets are not rotated since there is only the first partition per group, empty group does not get a partition
        Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred1", "type1", "type1"), Predicate("pred3", "type3", "type3"), Predicate("pred2", "type2", "type2"))), None, None, model),

        Partition(Seq(Target("host4:9080"), Target("host5:9080")), Some(Set(Predicate("pred5", "type5", "type5"), Predicate("pred4", "type4", "type4"))), None, None, model),

        Partition(Seq(Target("host6:9080")), Some(Set(Predicate("pred6", "type6", "type6"))), None, None, model)
      ))
    }

    it("should simplify promised with optional filters") {
      val partitions =
        PredicatePartitioner(schema, clusterState, 5)
          .withFilters(Filters(
            Seq(PredicateNameIsIn("pred3"), ObjectValueIsIn("value")),
            Seq(ObjectTypeIsIn("type2"))
          ))
          .getPartitions(model)

      // optional ObjectTypeIsIn("type2") is replaced with PredicateNameIsIn("pred2")
      // which simplifies with promised PredicateNameIsIn("pred3") to AlwaysFalse,
      // which results in no partitions empty result)
      assert(partitions === Seq())
    }

  }
}
