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

    it("should rotate Seq left") {
      val seq = 0 until 5
      assert(seq.rotateLeft(-2147483648) === (2 until 5) ++ (0 until 2))
      assert(seq.rotateLeft(-2147483647) === (3 until 5) ++ (0 until 3))
      assert(seq.rotateLeft(-2147483646) === (4 until 5) ++ (0 until 4))
      assert(seq.rotateLeft(-2147483645) === (0 until 5))
      assert(seq.rotateLeft(-1000010) === (0 until 5))
      assert(seq.rotateLeft(-1000009) === (1 until 5) ++ (0 until 1))
      assert(seq.rotateLeft(-1000008) === (2 until 5) ++ (0 until 2))
      assert(seq.rotateLeft(-1000007) === (3 until 5) ++ (0 until 3))
      assert(seq.rotateLeft(-1000006) === (4 until 5) ++ (0 until 4))
      assert(seq.rotateLeft(-1000005) === (0 until 5))
      assert(seq.rotateLeft(-5) === (0 until 5))
      assert(seq.rotateLeft(-4) === (1 until 5) ++ (0 until 1))
      assert(seq.rotateLeft(-3) === (2 until 5) ++ (0 until 2))
      assert(seq.rotateLeft(-2) === (3 until 5) ++ (0 until 3))
      assert(seq.rotateLeft(-1) === (4 until 5) ++ (0 until 4))
      assert(seq.rotateLeft(0) === (0 until 5))
      assert(seq.rotateLeft(1) === (1 until 5) ++ (0 until 1))
      assert(seq.rotateLeft(2) === (2 until 5) ++ (0 until 2))
      assert(seq.rotateLeft(3) === (3 until 5) ++ (0 until 3))
      assert(seq.rotateLeft(4) === (4 until 5) ++ (0 until 4))
      assert(seq.rotateLeft(5) === (0 until 5))
      assert(seq.rotateLeft(6) === (1 until 5) ++ (0 until 1))
      assert(seq.rotateLeft(7) === (2 until 5) ++ (0 until 2))
      assert(seq.rotateLeft(8) === (3 until 5) ++ (0 until 3))
      assert(seq.rotateLeft(9) === (4 until 5) ++ (0 until 4))
      assert(seq.rotateLeft(1000000) === (0 until 5))
      assert(seq.rotateLeft(1000001) === (1 until 5) ++ (0 until 1))
      assert(seq.rotateLeft(1000002) === (2 until 5) ++ (0 until 2))
      assert(seq.rotateLeft(1000003) === (3 until 5) ++ (0 until 3))
      assert(seq.rotateLeft(1000004) === (4 until 5) ++ (0 until 4))
      assert(seq.rotateLeft(1000005) === (0 until 5))
      assert(seq.rotateLeft(2147483645) === (0 until 5))
      assert(seq.rotateLeft(2147483646) === (1 until 5) ++ (0 until 1))
      assert(seq.rotateLeft(2147483647) === (2 until 5) ++ (0 until 2))
    }

    it("should rotate Seq right") {
      val seq = 0 until 5
      assert(seq.rotateRight(-2147483648) === (3 until 5) ++ (0 until 3))
      assert(seq.rotateRight(-2147483647) === (2 until 5) ++ (0 until 2))
      assert(seq.rotateRight(-2147483646) === (1 until 5) ++ (0 until 1))
      assert(seq.rotateRight(-2147483645) === (0 until 5))
      assert(seq.rotateRight(-1000010) === (0 until 5))
      assert(seq.rotateRight(-1000009) === (4 until 5) ++ (0 until 4))
      assert(seq.rotateRight(-1000008) === (3 until 5) ++ (0 until 3))
      assert(seq.rotateRight(-1000007) === (2 until 5) ++ (0 until 2))
      assert(seq.rotateRight(-1000006) === (1 until 5) ++ (0 until 1))
      assert(seq.rotateRight(-1000005) === (0 until 5))
      assert(seq.rotateRight(-5) === (0 until 5))
      assert(seq.rotateRight(-4) === (4 until 5) ++ (0 until 4))
      assert(seq.rotateRight(-3) === (3 until 5) ++ (0 until 3))
      assert(seq.rotateRight(-2) === (2 until 5) ++ (0 until 2))
      assert(seq.rotateRight(-1) === (1 until 5) ++ (0 until 1))
      assert(seq.rotateRight(0) === (0 until 5))
      assert(seq.rotateRight(1) === (4 until 5) ++ (0 until 4))
      assert(seq.rotateRight(2) === (3 until 5) ++ (0 until 3))
      assert(seq.rotateRight(3) === (2 until 5) ++ (0 until 2))
      assert(seq.rotateRight(4) === (1 until 5) ++ (0 until 1))
      assert(seq.rotateRight(5) === (0 until 5))
      assert(seq.rotateRight(6) === (4 until 5) ++ (0 until 4))
      assert(seq.rotateRight(7) === (3 until 5) ++ (0 until 3))
      assert(seq.rotateRight(8) === (2 until 5) ++ (0 until 2))
      assert(seq.rotateRight(9) === (1 until 5) ++ (0 until 1))
      assert(seq.rotateRight(1000000) === (0 until 5))
      assert(seq.rotateRight(1000001) === (4 until 5) ++ (0 until 4))
      assert(seq.rotateRight(1000002) === (3 until 5) ++ (0 until 3))
      assert(seq.rotateRight(1000003) === (2 until 5) ++ (0 until 2))
      assert(seq.rotateRight(1000004) === (1 until 5) ++ (0 until 1))
      assert(seq.rotateRight(1000005) === (0 until 5))
      assert(seq.rotateRight(2147483645) === (0 until 5))
      assert(seq.rotateRight(2147483646) === (4 until 5) ++ (0 until 4))
      assert(seq.rotateRight(2147483647) === (3 until 5) ++ (0 until 3))
    }

    it("should rotate empty Seq") {
      Seq(Integer.MIN_VALUE, Integer.MIN_VALUE-1, -2, -1, 0, +1, +2, Integer.MAX_VALUE-1, Integer.MAX_VALUE)
        .foreach(i => assert(Seq.empty.rotateLeft(i) === Seq.empty))
      Seq(Integer.MIN_VALUE, Integer.MIN_VALUE-1, -2, -1, 0, +1, +2, Integer.MAX_VALUE-1, Integer.MAX_VALUE)
        .foreach(i => assert(Seq.empty.rotateRight(i) === Seq.empty))
    }

  }

}
