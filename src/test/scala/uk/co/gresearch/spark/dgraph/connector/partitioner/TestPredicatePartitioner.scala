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
    val execution = DgraphExecutorProvider(None)
    val encoder = TypedTripleEncoder(schema.predicateMap)
    implicit val model: TripleTableModel = TripleTableModel(execution, encoder, ChunkSizeDefault)
    val simpleClusterState = ClusterState(
      Map("1" -> Set(Target("host1:9080"), Target("host2:9080"), Target("host3:9080"))),
      Map("1" -> Set("pred1", "pred2", "pred3", "pred4", "pred5", "pred6")),
      10000,
      UUID.randomUUID()
    )

    it("should partition with 1 predicates per partition") {
      val partitioner = PredicatePartitioner(schema, clusterState, 1)
      val partitions = partitioner.getPartitions

      assert(partitions.toSet === Set(
        // predicates are shuffled within group, targets rotate within group, empty group does not get a partition
        Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set("pred1"), Set.empty).getAll,
        Partition(Seq(Target("host3:9080"), Target("host2:9080"))).has(Set("pred3"), Set.empty).getAll,
        Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set("pred2"), Set.empty).getAll,

        Partition(Seq(Target("host4:9080"), Target("host5:9080"))).has(Set("pred5"), Set.empty).getAll,
        Partition(Seq(Target("host5:9080"), Target("host4:9080"))).has(Set("pred4"), Set.empty).getAll,

        Partition(Seq(Target("host6:9080"))).has(Set("pred6"), Set.empty).getAll,
      ))
    }

    it("should partition with 2 predicates per partition") {
      val partitioner = PredicatePartitioner(schema, clusterState, 2)
      val partitions = partitioner.getPartitions

      assert(partitions.toSet === Set(
        // predicates are shuffled within group, targets rotate within group, empty group does not get a partition
        Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set("pred1", "pred2"), Set.empty).getAll,
        Partition(Seq(Target("host3:9080"), Target("host2:9080"))).has(Set("pred3"), Set.empty).getAll,

        Partition(Seq(Target("host4:9080"), Target("host5:9080"))).has(Set("pred5", "pred4"), Set.empty).getAll,

        Partition(Seq(Target("host6:9080"))).has(Set("pred6"), Set.empty).getAll
      ))
    }

    Seq(3, 4, 7).foreach(predsPerPart =>
      it(s"should partition with $predsPerPart predicates per partition") {
        val partitioner = PredicatePartitioner(schema, clusterState, predsPerPart)
        val partitions = partitioner.getPartitions

        assert(partitions === Seq(
          // predicates are shuffled within group, targets are not rotated since there is only the first partition per group, empty group does not get a partition
          Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set("pred1", "pred3", "pred2"), Set.empty).getAll,

          Partition(Seq(Target("host4:9080"), Target("host5:9080"))).has(Set("pred5", "pred4"), Set.empty).getAll,

          Partition(Seq(Target("host6:9080"))).has(Set("pred6"), Set.empty).getAll
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

    it("should apply SubjectIsIn filter") {
      val partitioner = PredicatePartitioner(schema, clusterState, 5)
      val partitions = partitioner.withFilters(Filters.fromPromised(SubjectIsIn(Uid("0x1")))).getPartitions
      assert(partitions === Seq(
        Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set("pred1", "pred2", "pred3"), Set.empty).uids(Uid("0x1")).getAll,
        Partition(Seq(Target("host4:9080"), Target("host5:9080"))).has(Set("pred4", "pred5"), Set.empty).uids(Uid("0x1")).getAll,
        Partition(Seq(Target("host6:9080"))).has(Set("pred6"), Set.empty).uids(Uid("0x1")).getAll
      ))
    }

    it("should apply IntersectPredicateNameIsIn filter") {
      val partitioner = PredicatePartitioner(schema, clusterState, 5)
      val partitions1 = partitioner.withFilters(Filters.fromPromised(IntersectPredicateNameIsIn("pred3"))).getPartitions
      assert(partitions1 === Seq(Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set("pred3"), Set.empty).getAll))

      val partitions2 = partitioner.withFilters(Filters.fromPromised(IntersectPredicateNameIsIn("pred2", "pred3", "pred4"))).getPartitions
      assert(partitions2 === Seq(
        Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set("pred2", "pred3"), Set.empty).getAll,
        Partition(Seq(Target("host4:9080"), Target("host5:9080"))).has(Set("pred4"), Set.empty).getAll
      ))
    }

    it("should apply PredicateNameIs filter (single-group cluster)") {
      val partitioner = PredicatePartitioner(schema, simpleClusterState, schema.predicates.size)
      val partitions1 = partitioner.withFilters(Filters.fromPromised(PredicateNameIs("pred3"))).getPartitions
      assert(partitions1 === Seq(
        Partition(Seq(Target("host1:9080"), Target("host2:9080"), Target("host3:9080")))
          .has(Set("pred3"), Set.empty)
          .get(schema.predicates)
      ))

      val partitions2 = partitioner.withFilters(Filters.fromPromised(PredicateNameIs("pred2"), PredicateNameIs("pred3"), PredicateNameIs("pred4"))).getPartitions
      assert(partitions2 === Seq(
        Partition(Seq(Target("host1:9080"), Target("host2:9080"), Target("host3:9080")))
          .has(Set("pred2"), Set.empty)
          .has(Set("pred3"), Set.empty)
          .has(Set("pred4"), Set.empty)
          .get(schema.predicates)
      ))
    }

    it("should apply IntersectPredicateValueIsIn filter") {
      val partitioner = PredicatePartitioner(schema, clusterState, 5)
      val partitions1 = partitioner.withFilters(Filters.fromPromised(IntersectPredicateValueIsIn(Set("pred3"), Set("value")))).getPartitions
      assert(partitions1 === Seq(Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set("pred3"), Set.empty).eq("pred3", Set[Any]("value")).getAll))

      val partitions2 = partitioner.withFilters(Filters.fromPromised(IntersectPredicateValueIsIn(Set("pred2", "pred3", "pred4"), Set("value")))).getPartitions
      assert(partitions2 === Seq(
        Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set("pred2", "pred3"), Set.empty).eq(Set("pred2", "pred3", "pred4"), Set[Any]("value")).getAll,
        Partition(Seq(Target("host4:9080"), Target("host5:9080"))).has(Set("pred4"), Set.empty).eq(Set("pred2", "pred3", "pred4"), Set[Any]("value")).getAll
      ))
    }

    it("should apply SinglePredicateValueIsIn filter (single-group cluster)") {
      val partitioner = PredicatePartitioner(schema, simpleClusterState, schema.predicates.size)
      val partitions1 = partitioner.withFilters(Filters.fromPromised(SinglePredicateValueIsIn("pred3", Set("value")))).getPartitions
      assert(partitions1 === Seq(
        Partition(Seq(Target("host1:9080"), Target("host2:9080"), Target("host3:9080")))
          .has(Set("pred3"), Set.empty)
          .eq("pred3", Set[Any]("value"))
          .get(schema.predicates)
      ))

      val partitions2 = partitioner.withFilters(Filters.fromPromised(SinglePredicateValueIsIn("pred2", Set("value2")), SinglePredicateValueIsIn("pred3", Set("value3")), SinglePredicateValueIsIn("pred4", Set("value4")))).getPartitions
      assert(partitions2 === Seq(
        Partition(Seq(Target("host1:9080"), Target("host2:9080"), Target("host3:9080")))
          .has(Set("pred2"), Set.empty)
          .has(Set("pred3"), Set.empty)
          .has(Set("pred4"), Set.empty)
          .eq("pred2", Set[Any]("value2"))
          .eq("pred3", Set[Any]("value3"))
          .eq("pred4", Set[Any]("value4"))
          .get(schema.predicates)
      ))
    }

    it("should apply ObjectTypeIsIn filter") {
      val partitioner = PredicatePartitioner(schema, clusterState, 5)
      val partitions1 = partitioner.withFilters(Filters.fromPromised(ObjectTypeIsIn("type3"))).getPartitions
      assert(partitions1 === Seq(Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set("pred3"), Set.empty).getAll))

      val partitions2 = partitioner.withFilters(Filters.fromPromised(ObjectTypeIsIn("type2", "type3", "type4"))).getPartitions
      assert(partitions2 === Seq(
        Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set("pred2", "pred3"), Set.empty).getAll,
        Partition(Seq(Target("host4:9080"), Target("host5:9080"))).has(Set("pred4"), Set.empty).getAll
      ))
    }

    it("should apply ObjectValueIsIn with IntersectPredicateNameIsIn filter") {
      val partitioner = PredicatePartitioner(schema, clusterState, 5)
      val partitions1 = partitioner.withFilters(Filters.fromPromised(IntersectPredicateNameIsIn("pred3"), ObjectValueIsIn("value"))).getPartitions
      assert(partitions1 === Seq(Partition(Seq(Target("host2:9080"), Target("host3:9080")), Set(Has(Set("pred3"), Set.empty), IsIn(Set("pred3"), Set[Any]("value")), Get(Set("pred3"), Set.empty)))))

      val partitions2 = partitioner.withFilters(Filters.fromPromised(IntersectPredicateNameIsIn("pred2", "pred3", "pred4"), ObjectValueIsIn("value1", "value2"))).getPartitions
      assert(partitions2 === Seq(
        Partition(Seq(Target("host2:9080"), Target("host3:9080")), Set(Has(Set("pred2", "pred3"), Set.empty), IsIn(Set("pred2", "pred3", "pred4"), Set[Any]("value1", "value2")), Get(Set("pred2", "pred3"), Set.empty))),
        Partition(Seq(Target("host4:9080"), Target("host5:9080")), Set(Has(Set("pred4"), Set.empty), IsIn(Set("pred2", "pred3", "pred4"), Set[Any]("value1", "value2")), Get(Set("pred4"), Set.empty)))
      ))
    }

    it("should apply ObjectValueIsIn with PredicateNameIs filter (single-group cluster)") {
      val partitioner = PredicatePartitioner(schema, simpleClusterState, schema.predicates.size)
      val partitions1 = partitioner.withFilters(Filters.fromPromised(PredicateNameIs("pred3"), ObjectValueIsIn("value"))).getPartitions
      assert(partitions1 === Seq(
        Partition(Seq(Target("host1:9080"), Target("host2:9080"), Target("host3:9080")))
          .has(Set("pred3"), Set.empty)
          .eq("pred3", Set[Any]("value"))
          .get(schema.predicates)
      ))
    }

    it("should apply ObjectValueIsIn with ObjectTypeIsIn filter") {
      val partitioner = PredicatePartitioner(schema, clusterState, 5)
      val partitions1 = partitioner.withFilters(Filters.fromPromised(ObjectTypeIsIn("type3"), ObjectValueIsIn("value"))).getPartitions
      assert(partitions1 === Seq(Partition(Seq(Target("host2:9080"), Target("host3:9080")), Set(Has(Set("pred3"), Set.empty), IsIn("pred3", Set[Any]("value")), Get(Set("pred3"), Set.empty)))))

      val partitions2 = partitioner.withFilters(Filters.fromPromised(ObjectTypeIsIn("type2", "type3", "type4"), ObjectValueIsIn("value1", "value2"))).getPartitions
      assert(partitions2 === Seq(
        Partition(Seq(Target("host2:9080"), Target("host3:9080")), Set(Has(Set("pred2", "pred3"), Set.empty), IsIn(Set("pred2", "pred3", "pred4"), Set[Any]("value1", "value2")), Get(Set("pred2", "pred3"), Set.empty))),
        Partition(Seq(Target("host4:9080"), Target("host5:9080")), Set(Has(Set("pred4"), Set.empty), IsIn(Set("pred2", "pred3", "pred4"), Set[Any]("value1", "value2")), Get(Set("pred4"), Set.empty)))
      ))
    }

    it("should not apply ObjectValueIsIn filter only") {
      val partitioner = PredicatePartitioner(schema, clusterState, 5)
      val partitions = partitioner.withFilters(Filters.fromOptional(ObjectValueIsIn("value"))).getPartitions

      // same as in s"should partition with $predsPerPart predicates per partition" above
      assert(partitions === Seq(
        // predicates are shuffled within group, targets are not rotated since there is only the first partition per group, empty group does not get a partition
        Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set("pred1", "pred3", "pred2"), Set.empty).getAll,

        Partition(Seq(Target("host4:9080"), Target("host5:9080"))).has(Set("pred5", "pred4"), Set.empty).getAll,

        Partition(Seq(Target("host6:9080"))).has(Set("pred6"), Set.empty).getAll
      ))
    }

    it("should simplify promised with optional filters") {
      val partitions =
        PredicatePartitioner(schema, clusterState, 5)
          .withFilters(Filters(
            Set(IntersectPredicateNameIsIn("pred3"), ObjectValueIsIn("value")),
            Set(ObjectTypeIsIn("type2"))
          ))
          .getPartitions

      // optional ObjectTypeIsIn("type2") is replaced with IntersectPredicateNameIsIn("pred2")
      // which simplifies with promised IntersectPredicateNameIsIn("pred3") to AlwaysFalse,
      // which results in no partitions empty result)
      assert(partitions === Seq())
    }

    val subjectPredicate = Seq(Predicate("uid", "subject", "uid"))

    it("should partition with 2 predicates per partition and project predicates") {
      val partitioner = PredicatePartitioner(schema, clusterState, 2)
        .withProjection(subjectPredicate ++ schema.predicates.filter(p => Set("pred2", "pred3", "pred4").contains(p.predicateName)))

      val partitions = partitioner.getPartitions
      assert(partitions === Seq(
        // predicates are shuffled within group, targets rotate within group, empty group does not get a partition
        Partition(Seq(Target("host2:9080"), Target("host3:9080"))).has(Set("pred1", "pred2"), Set.empty).get(Set("pred2"), Set.empty),
        Partition(Seq(Target("host3:9080"), Target("host2:9080"))).has(Set("pred3"), Set.empty).getAll,

        Partition(Seq(Target("host4:9080"), Target("host5:9080"))).has(Set("pred5", "pred4"), Set.empty).get(Set("pred4"), Set.empty),

        Partition(Seq(Target("host6:9080"))).has(Set("pred6"), Set.empty).get(Set.empty, Set.empty)
      ))
    }

  }
}
