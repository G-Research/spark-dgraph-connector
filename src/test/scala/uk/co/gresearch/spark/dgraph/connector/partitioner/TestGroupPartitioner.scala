package uk.co.gresearch.spark.dgraph.connector.partitioner

import java.util.UUID

import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Partition, Predicate, Schema, Target}

class TestGroupPartitioner extends FunSpec {

  describe("GroupPartitioner") {

    val schema = Schema((1 to 4).map(i => Predicate(s"pred$i", s"type$i")).toSet)
    val clusterState = ClusterState(
      Map(
        "1" -> Set(Target("host1:9080")),
        "2" -> Set(Target("host2:9080"), Target("host3:9080")),
        "3" -> Set(Target("host4:9080"))
      ),
      Map(
        "1" -> Set.empty,
        "2" -> Set("pred1", "pred2", "pred3"),
        "3" -> Set("pred4")
      ),
      10000,
      UUID.randomUUID()
    )

    it("should partition") {
      val partitioner = GroupPartitioner(schema, clusterState)
      val partitions = partitioner.getPartitions

      assert(partitions.length === 2)
      assert(partitions.toSet === Set(
        Partition(Seq(Target("host2:9080"), Target("host3:9080")), Some(Set(Predicate("pred1", "type1"), Predicate("pred2", "type2"), Predicate("pred3", "type3"))), None),
        Partition(Seq(Target("host4:9080")), Some(Set(Predicate("pred4", "type4"))), None)
      ))
    }

  }

}
