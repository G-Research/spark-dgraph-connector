package uk.co.gresearch.spark.dgraph.connector.partitioner

import java.util.UUID

import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Partition, Predicate, Schema, Target}
import uk.co.gresearch.spark.dgraph.connector.partitioner.PredicatePartitioner._

class TestPredicatePartitioner extends FunSpec {

  describe("PredicatePartitioner") {

    val P = 10
    assert(P > 2)
    val predicates = (1 to P).map(i => Predicate(s"pred$i", s"type$i")).toSet

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

    it("should rotate Seq") {
      val seq = 0 until 5
      assert(seq.rotate(-2) === (3 until 5) ++ (0 until 3))
      assert(seq.rotate(-1) === (4 until 5) ++ (0 until 4))
      assert(seq.rotate(0) === (0 until 5))
      assert(seq.rotate(1) === (1 until 5) ++ (0 until 1))
      assert(seq.rotate(2) === (2 until 5) ++ (0 until 2))
      assert(seq.rotate(3) === (3 until 5) ++ (0 until 3))
      assert(seq.rotate(4) === (4 until 5) ++ (0 until 4))
      assert(seq.rotate(5) === (0 until 5))
      assert(seq.rotate(6) === (1 until 5) ++ (0 until 1))
      assert(seq.rotate(7) === (2 until 5) ++ (0 until 2))
    }

    it("should rotate empty Seq") {
      (-2 to +2).foreach(i => assert(Seq.empty.rotate(i) === Seq.empty))
    }

    val schema = Schema((1 to 6).map(i => Predicate(s"pred$i", s"type$i")).toSet)
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

    it("should partition with 1 predicates per partition") {
      val partitioner = new PredicatePartitioner(schema, clusterState, 1)
      val partitions = partitioner.getPartitions

      assert(partitions.length === 6)
      assert(partitions.toSet === Set(
        // predicates are shuffled within group, targets rotate within group, empty group does not get a partition
        Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred1", "type1")))),
        Partition(Seq(Target("host3:9080"), Target("host2:9080")), Some(Set(Predicate("pred3", "type3")))),
        Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred2", "type2")))),

        Partition(Seq(Target("host4:9080"), Target("host5:9080")), Some(Set(Predicate("pred5", "type5")))),
        Partition(Seq(Target("host5:9080"), Target("host4:9080")), Some(Set(Predicate("pred4", "type4")))),

        Partition(Seq(Target("host6:9080")), Some(Set(Predicate("pred6", "type6")))),
      ))
    }

    it("should partition with 2 predicates per partition") {
      val partitioner = new PredicatePartitioner(schema, clusterState, 2)
      val partitions = partitioner.getPartitions

      assert(partitions.length === 4)
      assert(partitions.toSet === Set(
        // predicates are shuffled within group, targets rotate within group, empty group does not get a partition
        Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred1", "type1"), Predicate("pred2", "type2")))),
        Partition(Seq(Target("host3:9080"), Target("host2:9080")), Some(Set(Predicate("pred3", "type3")))),

        Partition(Seq(Target("host4:9080"), Target("host5:9080")), Some(Set(Predicate("pred5", "type5"), Predicate("pred4", "type4")))),

        Partition(Seq(Target("host6:9080")), Some(Set(Predicate("pred6", "type6"))))
      ))
    }

    Seq(3, 4, 7).foreach(predsPerPart =>
      it(s"should partition with $predsPerPart predicates per partition") {
        val partitioner = new PredicatePartitioner(schema, clusterState, predsPerPart)
        val partitions = partitioner.getPartitions

        assert(partitions.length === 3)
        assert(partitions === Seq(
          // predicates are shuffled within group, targets are not rotated since there is only the first partition per group, empty group does not get a partition
          Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred1", "type1"), Predicate("pred3", "type3"), Predicate("pred2", "type2")))),

          Partition(Seq(Target("host4:9080"), Target("host5:9080")), Some(Set(Predicate("pred5", "type5"), Predicate("pred4", "type4")))),

          Partition(Seq(Target("host6:9080")), Some(Set(Predicate("pred6", "type6"))))
        ))
      }
    )

    it("should fail with negative or zero predsPerPart") {
      assertThrows[IllegalArgumentException] {
        new PredicatePartitioner(schema, clusterState, -1)
      }
      assertThrows[IllegalArgumentException] {
        new PredicatePartitioner(schema, clusterState, 0)
      }
    }

  }
}
