/*
 * Copyright 2020 G-Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector

import com.google.common.primitives.UnsignedLong
import org.scalatest.funspec.AnyFunSpec

class TestConnector extends AnyFunSpec {

  describe("Connector") {

    Map(
        TriplesSource -> "triples",
        EdgesSource -> "edges",
        NodesSource -> "nodes"
    ).foreach {
      case (pkg, source) =>
        it(s"should provide $source source package name") {
          assert(pkg === s"uk.co.gresearch.spark.dgraph.$source")
        }
    }

    Seq(
      (Uid(UnsignedLong.valueOf(0)), "0", "0x0", true, false, UnsignedLong.ONE, UnsignedLong.ZERO.minus(UnsignedLong.ONE)),
      (Uid.apply(UnsignedLong.valueOf(1).asInstanceOf[Any]), "1", "0x1", true, false, UnsignedLong.valueOf(2), UnsignedLong.ZERO),
      (Uid(2L), "2", "0x2", true, false, UnsignedLong.valueOf(3), UnsignedLong.ONE),
      (Uid(3), "3", "0x3", false, true, UnsignedLong.valueOf(4), UnsignedLong.valueOf(2)),
      (Uid("0x4"), "4", "0x4", false, true, UnsignedLong.valueOf(5), UnsignedLong.valueOf(3)),
      (Uid("0x005"), "5", "0x5", false, true, UnsignedLong.valueOf(6), UnsignedLong.valueOf(4)),
      (Uid(UnsignedLong.fromLongBits(8214320560726473464L)), "8214320560726473464", "0x71ff21075549faf8", false, true, UnsignedLong.valueOf(8214320560726473465L), UnsignedLong.valueOf(8214320560726473463L)),
      (Uid(UnsignedLong.fromLongBits(-6346846686373277921L)), "-6346846686373277921", "0xa7eb7890d6db0f1f", false, true, UnsignedLong.fromLongBits(-6346846686373277920L), UnsignedLong.fromLongBits(-6346846686373277922L)),
    ).foreach { case (uid, str, hex, lt, ge, next, before) =>
      it(s"should handle Uid $hex") {
        val cmp = Uid(UnsignedLong.valueOf(3))
        assert(uid.toString === str)
        assert(uid.toHexString === hex)
        assert(uid < cmp === lt)
        assert(uid >= cmp === ge)
        assert(uid.next.uid === next)
        assert(uid.before.uid === before)
      }
    }

    it("should validate Uid") {
      assertThrows[IllegalArgumentException] { Uid(-1) }
      assertThrows[IllegalArgumentException] { Uid("0x-1") }
      assertThrows[IllegalArgumentException] { Uid("123") }
      assertThrows[IllegalArgumentException] { Uid("0xyz") }
      assertThrows[IllegalArgumentException] { Uid("0x10000000000000000") }
    }

    Seq(
      (UidRange(Uid(1), Uid(2)), UnsignedLong.valueOf(1)),
      (UidRange(Uid(2), Uid(10)), UnsignedLong.valueOf(8)),
      (UidRange(Uid(8214320560726473464L), Uid(UnsignedLong.fromLongBits(-6346846686373277921L))), UnsignedLong.valueOf(3885576826609800231L)),
      (UidRange(Uid(UnsignedLong.fromLongBits(-6346846686373277921L)), Uid(UnsignedLong.fromLongBits(-1877623327044447073L))), UnsignedLong.valueOf(4469223359328830848L)),
    ).foreach { case (range, length) =>
      it(s"should handle UidRange $range") {
        assert(range.length === length)
      }
    }

    it("should validate UidRange") {
      assertThrows[IllegalArgumentException]{ UidRange(Uid(1), Uid(1)) }
      assertThrows[IllegalArgumentException]{ UidRange(Uid(2), Uid(1)) }
      assertThrows[IllegalArgumentException]{ UidRange(Uid(UnsignedLong.fromLongBits(-6346846686373277921L)), Uid(8214320560726473464L)) }
      assertThrows[IllegalArgumentException]{ UidRange(Uid(UnsignedLong.fromLongBits(-1877623327044447073L)), Uid(UnsignedLong.fromLongBits(-6346846686373277921L))) }
    }

    it("should validate Chunk") {
      assertThrows[IllegalArgumentException] { Chunk(Uid(0), 0) }
      assertThrows[IllegalArgumentException] { Chunk(Uid(0), -1) }
      assertThrows[IllegalArgumentException] { Chunk(Uid(0), Int.MinValue) }
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
