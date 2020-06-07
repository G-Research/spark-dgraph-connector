package uk.co.gresearch.spark.dgraph.connector.partitioner

import java.util.UUID

import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.partitioner.PredicatePartitioner._
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

    it("should partition with 1 partition per alpha") {
      val partitioner = AlphaPartitioner(schema, clusterState, 1)
      val partitions = partitioner.getPartitions

      assert(partitions.length === 4)
      assert(partitions.toSet === Set(
        // predicates are shuffled within group and alpha, targets rotate within group, empty group does not get a partition
        Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred1", "type1"), Predicate("pred2", "type2"))), None),
        Partition(Seq(Target("host3:9080"), Target("host2:9080")), Some(Set(Predicate("pred3", "type3"), Predicate("pred4", "type4"))), None),

        Partition(Seq(Target("host4:9080"), Target("host5:9080")), Some(Set(Predicate("pred5", "type5"))), None),

        Partition(Seq(Target("host6:9080")), Some(Set(Predicate("pred7", "type7"), Predicate("pred6", "type6"))), None)
      ))
    }

    Seq(2, 3, 7).foreach(partsPerAlpha =>
      it(s"should partition with $partsPerAlpha partitions per alpha") {
        val partitioner = AlphaPartitioner(schema, clusterState, 2)
        val partitions = partitioner.getPartitions

        assert(partitions.length === 7)
        assert(partitions.toSet === Set(
          // predicates are shuffled within group and alpha, targets rotate within group, empty group does not get a partition
          Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred1", "type1"))), None),
          Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred2", "type2"))), None),
          Partition(Seq(Target("host3:9080"), Target("host2:9080")), Some(Set(Predicate("pred3", "type3"))), None),
          Partition(Seq(Target("host3:9080"), Target("host2:9080")), Some(Set(Predicate("pred4", "type4"))), None),

          Partition(Seq(Target("host4:9080"), Target("host5:9080")), Some(Set(Predicate("pred5", "type5"))), None),

          Partition(Seq(Target("host6:9080")), Some(Set(Predicate("pred6", "type6"))), None),
          Partition(Seq(Target("host6:9080")), Some(Set(Predicate("pred7", "type7"))), None)
        ))
      }
    )

    it("should fail with negative or zero partsPerAlpha") {
      assertThrows[IllegalArgumentException]{ AlphaPartitioner(schema, clusterState, -1) }
      assertThrows[IllegalArgumentException]{ AlphaPartitioner(schema, clusterState, 0) }
    }

  }

}
