package uk.co.gresearch.spark.dgraph.graphframes

import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster

class TestGraphFrames extends FunSpec
  with SparkTestSession with DgraphTestCluster {

  describe("GraphFrames") {

    Seq(
      ("target", () => Seq(cluster.grpc)),
      ("targets", () => Seq(cluster.grpc, cluster.grpcLocalIp))
    ).foreach{case (test, targets_func) =>

      lazy val targets = targets_func()

      it(s"should load dgraph from $test via implicit session") {
        val graph = loadGraph(targets: _*)
        val pageRank = graph.pageRank.maxIter(10)
        pageRank.run().triplets.show(false)
      }

      it(s"should load dgraph from $test via reader") {
        val graph = spark.read.dgraph(targets: _*)
        val pageRank = graph.pageRank.maxIter(10)
        pageRank.run().triplets.show(false)
      }

      it(s"should load vertices from $test via implicit session") {
        val vertices = loadVertices(targets: _*)
        vertices.show(false)
      }

      it(s"should load vertices from $test via reader") {
        val vertices = spark.read.dgraphVertices(targets: _*)
        vertices.show(false)
      }

      it(s"should load edges from $test via implicit session") {
        val edges = loadEdges(targets: _*)
        edges.show(false)
      }

      it(s"should load edges from $test via reader") {
        val edges = spark.read.dgraphEdges(targets: _*)
        edges.show(false)
      }

    }

  }

}
