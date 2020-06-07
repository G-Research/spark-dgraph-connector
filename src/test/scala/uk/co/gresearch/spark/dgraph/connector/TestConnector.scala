package uk.co.gresearch.spark.dgraph.connector

import org.scalatest.FunSpec

class TestConnector extends FunSpec {

  describe("Connector") {

    Map(
      TriplesSource -> "triples",
      EdgesSource -> "edges",
      NodesSource -> "nodes"
    ).foreach {
      case (pkg, source) =>
        it(s"should provide $source source package name") {
          assert(pkg === s"uk.co.gresearch.spark.dgraph.connector.$source")
        }
    }

    it("should validate UidRange") {
      assertThrows[IllegalArgumentException]{ UidRange(-1, 1000) }
      assertThrows[IllegalArgumentException]{ UidRange(0, 0) }
      assertThrows[IllegalArgumentException]{ UidRange(0, -1) }
    }

  }

}
