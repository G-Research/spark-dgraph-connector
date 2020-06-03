package uk.co.gresearch.spark.dgraph

import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.connector._

class TestTriplesDataSource extends FunSpec with SparkTestSession {

  import spark.implicits._

  describe("TriplesDataSource") {

    it("should load triples via path") {
      spark
        .read
        .format(TriplesSource)
        .load("localhost:9080")
        .show(100, false)
    }

    it("should load triples via paths") {
      spark
        .read
        .format(TriplesSource)
        .load("localhost:9080", "127.0.0.1:9080")
        .show(100, false)
    }

    it("should load triples via target option") {
      spark
        .read
        .format(TriplesSource)
        .option(TargetOption, "localhost:9080")
        .load()
        .show(100, false)
    }

    it("should load triples via targets option") {
      spark
        .read
        .format(TriplesSource)
        .option(TargetsOption, "[\"localhost:9080\",\"127.0.0.1:9080\"]")
        .load()
        .show(100, false)
    }

    it("should load triples via implicit dgraph target") {
      spark
        .read
        .dgraphTriples("localhost:9080")
        .show(100, false)
    }

    it("should load triples via implicit dgraph targets") {
      spark
        .read
        .dgraphTriples("localhost:9080", "127.0.0.1:9080")
        .show(100, false)
    }

    it("should load string-object triples") {
      spark
        .read
        .option(TriplesModeOption, TriplesModeStringObjectsOption)
        .dgraphTriples("localhost:9080")
        .show(100, false)
    }

    it("should load typed-object triples") {
      spark
        .read
        .option(TriplesModeOption, TriplesModeTypedObjectsOption)
        .dgraphTriples("localhost:9080")
        .show(100, false)
    }

    it("should encode DGraphStringObjectRow") {
      val rows =
        spark
          .read
          .option(TriplesModeOption, TriplesModeStringObjectsOption)
          .dgraphTriples("localhost:9080")
          .as[DGraphStringObjectRow]
          .collectAsList()
      rows.forEach(println)
    }

    it("should encode DGraphTypedObjectRow") {
      val rows =
        spark
          .read
          .option(TriplesModeOption, TriplesModeTypedObjectsOption)
          .dgraphTriples("localhost:9080")
          .as[DGraphTypedObjectRow]
          .collectAsList()
      rows.forEach(println)
    }

    it("should fail without target") {
      assertThrows[IllegalArgumentException]{
        spark
          .read
          .format(TriplesSource)
          .load()
      }
    }

    it("should fail with unknown triple mode") {
      assertThrows[IllegalArgumentException]{
        spark
          .read
          .format(TriplesSource)
          .option(TriplesModeOption, "unknown")
          .load()
      }
    }

  }

}
