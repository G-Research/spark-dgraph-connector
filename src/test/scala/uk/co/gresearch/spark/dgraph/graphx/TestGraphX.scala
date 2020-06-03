package uk.co.gresearch.spark.dgraph.graphx

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.connector.Target

class TestGraphX extends FunSpec with SparkTestSession {

  implicit val session: SparkSession = spark

  describe("GraphX") {

    it("should load dgraph") {
      val graph = loadGraph(Target("localhost:9080"))
      graph.pageRank(0.0001).vertices.foreach(println)
    }

    it("should load vertices") {
      val vertices = loadVertices(Target("localhost:9080"))
      vertices.foreach(println)
    }

    it("should load edges") {
      val edges = loadEdges(Target("localhost:9080"))
      edges.foreach(println)
    }

  }

}
