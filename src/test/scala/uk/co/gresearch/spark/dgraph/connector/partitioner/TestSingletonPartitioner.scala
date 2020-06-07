package uk.co.gresearch.spark.dgraph.connector.partitioner

import java.util.UUID

import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Partition, Predicate, Schema, Target}

class TestSingletonPartitioner extends FunSpec {

  describe("SingletonPartitioner") {

    val targets = Seq(Target("host1:9080"), Target("host2:9080"))

    it("should partition") {
      val partitioner = SingletonPartitioner(targets)
      val partitions = partitioner.getPartitions

      assert(partitions.length === 1)
      assert(partitions.toSet === Set(Partition(targets, None, None)))
    }

  }

}
