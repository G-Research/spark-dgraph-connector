package uk.co.gresearch.spark.dgraph.graphx

import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.graphx._

class TestGraphX extends FunSpec with SparkTestSession {

  describe("GraphX") {

    Seq(
      ("target", Seq("localhost:9080")),
      ("targets", Seq("localhost:9080", "127.0.0.1:9080"))
    ).foreach{case (test, targets) =>

      it(s"should load dgraph from $test via implicit session") {
        val graph = loadGraph(targets: _*)
        val pageRank = graph.pageRank(0.0001)
        pageRank.vertices.foreach(println)
      }

      it(s"should load dgraph from $test via reader") {
        val graph = spark.read.dgraph(targets: _*)
        val pageRank = graph.pageRank(0.0001)
        pageRank.vertices.foreach(println)
      }

      it(s"should load vertices from $test via implicit session") {
        val vertices = loadVertices(targets: _*)
        vertices.foreach(println)
      }

      it(s"should load vertices from $test via reader") {
        val vertices = spark.read.dgraphVertices(targets: _*)
        vertices.foreach(println)
      }

      it(s"should load edges from $test via implicit session") {
        val edges = loadEdges(targets: _*)
        edges.foreach(println)
      }

      it(s"should load edges from $test via reader") {
        val edges = spark.read.dgraphEdges(targets: _*)
        edges.foreach(println)
      }

    }

  }

}
