package uk.co.gresearch.spark.dgraph.connector

import java.util.UUID

import org.scalatest.FunSpec

class TestClusterStateProvider extends FunSpec {

  describe("ClusterStateProvider") {

    it("should retrieve cluster state") {
      val provider = new ClusterStateProvider {}
      val state = provider.getClusterState(Target("localhost:9080"))
      assert(state.isDefined)
      assert(state.get === ClusterState(
        Map("1"-> Set(Target("localhost:9080"))),
        Map("1" -> Set("name", "dgraph.graphql.schema", "starring", "running_time", "release_date", "director", "revenue", "dgraph.type")),
        10000,
        UUID.fromString("5aacce50-a95f-440b-a32e-fbe6b4003980")
      ))
    }

    it("should retrieve cluster states") {
      val provider = new ClusterStateProvider {}
      val state = provider.getClusterState(Seq(Target("localhost:9080"), Target("127.0.0.1:9080")))
      assert(state === ClusterState(
        Map("1" -> Set(Target("localhost:9080"))),
        Map("1" -> Set("name", "dgraph.graphql.schema", "starring", "running_time", "release_date", "director", "revenue", "dgraph.type")),
        10000,
        UUID.fromString("5aacce50-a95f-440b-a32e-fbe6b4003980")
      ))
    }

    it("should fail for unavailable cluster") {
      val provider = new ClusterStateProvider {}
      assertThrows[RuntimeException] {
        provider.getClusterState(Seq(Target("localhost:1001")))
      }
    }

    it("should return None for unavailable cluster") {
      val provider = new ClusterStateProvider {}
      val state = provider.getClusterState(Target("localhost:1001"))
      assert(state.isEmpty)
    }

  }

}
