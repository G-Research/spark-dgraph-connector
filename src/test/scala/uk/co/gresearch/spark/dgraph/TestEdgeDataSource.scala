package uk.co.gresearch.spark.dgraph

import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.connector._

class TestEdgeDataSource extends FunSpec with SparkTestSession {

  import spark.implicits._

  describe("EdgeDataSource") {

    it("should load edges via path") {
      spark
        .read
        .format(EdgesSource)
        .load("localhost:9080")
        .show(100, false)
    }

    it("should load edges via paths") {
      spark
        .read
        .format(EdgesSource)
        .load("localhost:9080", "127.0.0.1:9080")
        .show(100, false)
    }

    it("should load edges via target option") {
      spark
        .read
        .format(EdgesSource)
        .option(TargetOption, "localhost:9080")
        .load()
        .show(100, false)
    }

    it("should load edges via targets option") {
      spark
        .read
        .format(EdgesSource)
        .option(TargetsOption, "[\"localhost:9080\",\"127.0.0.1:9080\"]")
        .load()
        .show(100, false)
    }

    it("should load edges via implicit dgraph target") {
      spark
        .read
        .dgraphEdges("localhost:9080")
        .show(100, false)
    }

    it("should load edges via implicit dgraph targets") {
      spark
        .read
        .dgraphEdges("localhost:9080", "127.0.0.1:9080")
        .show(100, false)
    }

    it("should encode DGraphEdgeRow") {
      val rows =
        spark
          .read
          .dgraphEdges("localhost:9080")
          .as[DGraphEdgeRow]
          .collectAsList()
      rows.forEach(println)
    }

    it("should fail without target") {
      assertThrows[IllegalArgumentException]{
        spark
          .read
          .format(EdgesSource)
          .load()
      }
    }

  }

}
