package uk.co.gresearch.spark.dgraph.connector.encoder

import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.{Triple, Uid}

class TestEdgeEncoder extends FunSpec {

  describe("EdgeEncoder") {

    it("should encode edges") {
      val encoder = EdgeEncoder()
      val edge = Triple(Uid(1), "predicate", Uid(2))
      val row = encoder.asInternalRow(edge)

      assert(row.numFields === 3)
      assert(row.getLong(0) === 1)
      assert(row.getUTF8String(1).toString === "predicate")
      assert(row.getLong(2) === 2)
    }

    it("should fail on node properties") {
      val encoder = EdgeEncoder()
      val edge = Triple(Uid(1), "predicate", 2L)
      assertThrows[IllegalArgumentException]{ encoder.asInternalRow(edge) }
    }

  }
}
