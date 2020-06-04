package uk.co.gresearch.spark.dgraph

import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.connector._

class TestNodeDataSource extends FunSpec with SparkTestSession {

  import spark.implicits._

  describe("NodeDataSource") {

    it("should load nodes via path") {
      spark
        .read
        .format(NodesSource)
        .load("localhost:9080")
        .show(100, false)
    }

    it("should load nodes via paths") {
      spark
        .read
        .format(NodesSource)
        .load("localhost:9080", "127.0.0.1:9080")
        .show(100, false)
    }

    it("should load nodes via target option") {
      spark
        .read
        .format(NodesSource)
        .option(TargetOption, "localhost:9080")
        .load()
        .show(100, false)
    }

    it("should load nodes via targets option") {
      spark
        .read
        .format(NodesSource)
        .option(TargetsOption, "[\"localhost:9080\",\"127.0.0.1:9080\"]")
        .load()
        .show(100, false)
    }

    it("should load nodes via implicit dgraph target") {
      spark
        .read
        .dgraphNodes("localhost:9080")
        .show(100, false)
    }

    it("should load nodes via implicit dgraph targets") {
      spark
        .read
        .dgraphNodes("localhost:9080", "127.0.0.1:9080")
        .show(100, false)
    }

    it("should encode TypedNode") {
      val rows =
        spark
          .read
          .format(NodesSource)
          .load("localhost:9080")
          .as[TypedNode]
          .collectAsList()
      rows.forEach(println)
    }

    it("should fail without target") {
      assertThrows[IllegalArgumentException]{
        spark
          .read
          .format(NodesSource)
          .load()
      }
    }

  }

}
