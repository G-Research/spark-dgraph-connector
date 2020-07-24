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

package uk.co.gresearch.spark.dgraph.connector.model

import com.google.gson.JsonArray
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.encoder.{JsonNodeInternalRowEncoder, StringTripleEncoder}
import uk.co.gresearch.spark.dgraph.connector.executor.{ExecutorProvider, JsonGraphQlExecutor}
import uk.co.gresearch.spark.dgraph.connector.model.TestChunkIterator.getChunk

class TestGraphTableModel extends FunSpec {

  describe("GraphTableModel") {

    val zeroUid = Uid("0x0")

    def chunkQuery(after: Uid): String =
      s"""{
         |  pred1 as var(func: has(<edge>), first: 3, after: ${after.toHexString})
         |  pred2 as var(func: has(<prop>), first: 3, after: ${after.toHexString})
         |
         |  result (func: uid(pred1,pred2), first: 3, after: ${after.toHexString}) {
         |    uid
         |    <edge> { uid }
         |    <prop>
         |  }
         |}""".stripMargin

    def chunkQueryUids(uids: Set[Uid], size: Int, after: Uid): String =
      s"""{
         |  result (func: uid(${uids.map(_.toHexString).mkString(",")}), first: $size, after: ${after.toHexString}) {
         |    uid
         |    <edge> { uid }
         |    <prop>
         |  }
         |}""".stripMargin

    val emptyChunkResult = """{"result":[]}"""

    val firstFullChunkResult =
      """{
        |  "result": [
        |    {
        |      "uid": "0x123",
        |      "edge": [{"uid": "0x123a"}],
        |      "prop": "str123"
        |    },
        |    {
        |      "uid": "0x124",
        |      "prop": "str124"
        |    },
        |    {
        |      "uid": "0x125",
        |      "edge": [{"uid": "0x125a"},{"uid": "0x125b"}]
        |    }
        |  ]
        |}
        |""".stripMargin
    val lastUidOfFirstFullChunk = Uid("0x125")
    val uidBeforeLastUidOfFirstFullChunk = Uid("0x124")

    val secondHalfChunkResult =
      """{
        |  "result": [
        |    {
        |      "uid": "0x132",
        |      "edge": [{"uid": "0x132a"}],
        |      "prop": "str132"
        |    },
        |    {
        |      "uid": "0x135",
        |      "edge": [{"uid": "0x135a"}],
        |      "prop": "str135"
        |    }
        |  ]
        |}
        |""".stripMargin
    val lastUidOfSecondHalfChunk = Uid("0x135")
    val uidBeforeLastUidOfSecondHalfChunk = Uid("0x132")

    val x124ChunkResult =
      """{
        |  "result": [
        |    {
        |      "uid": "0x124",
        |      "prop": "str124"
        |    }
        |  ]
        |}
        |""".stripMargin

    val x124x125ChunkResult =
      """{
        |  "result": [
        |    {
        |      "uid": "0x124",
        |      "prop": "str124"
        |    },
        |    {
        |      "uid": "0x125",
        |      "edge": [{"uid": "0x125a"},{"uid": "0x125b"}]
        |    }
        |  ]
        |}
        |""".stripMargin

    val x135ChunkResult =
      """{
        |  "result": [
        |    {
        |      "uid": "0x135",
        |      "edge": [{"uid": "0x135a"}],
        |      "prop": "str135"
        |    }
        |  ]
        |}
        |""".stripMargin

    val expecteds = Seq(
      InternalRow(Uid("0x123").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x123a").toString), UTF8String.fromString("uid")),
      InternalRow(Uid("0x123").uid, UTF8String.fromString("prop"), UTF8String.fromString("str123"), UTF8String.fromString("string")),
      InternalRow(Uid("0x124").uid, UTF8String.fromString("prop"), UTF8String.fromString("str124"), UTF8String.fromString("string")),
      InternalRow(Uid("0x125").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x125a").toString), UTF8String.fromString("uid")),
      InternalRow(Uid("0x125").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x125b").toString), UTF8String.fromString("uid")),
      InternalRow(Uid("0x132").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x132a").toString), UTF8String.fromString("uid")),
      InternalRow(Uid("0x132").uid, UTF8String.fromString("prop"), UTF8String.fromString("str132"), UTF8String.fromString("string")),
      InternalRow(Uid("0x135").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x135a").toString), UTF8String.fromString("uid")),
      InternalRow(Uid("0x135").uid, UTF8String.fromString("prop"), UTF8String.fromString("str135"), UTF8String.fromString("string"))
    )

    it("should read empty result") {
      val results = Map(chunkQuery(zeroUid) -> emptyChunkResult)
      doTest(results, Seq.empty)
    }

    it("should read empty chunk") {
      val results = Map(
        chunkQuery(zeroUid) -> firstFullChunkResult,
        chunkQuery(lastUidOfFirstFullChunk) -> emptyChunkResult
      )

      val expected = expecteds.filter(_.getLong(0) <= lastUidOfFirstFullChunk.uid)
      doTest(results, expected)
    }

    it("should read all chunks") {
      val results = Map(
        chunkQuery(zeroUid) -> firstFullChunkResult,
        chunkQuery(lastUidOfFirstFullChunk) -> secondHalfChunkResult
      )

      val expected = expecteds.filter(_.getLong(0) <= lastUidOfSecondHalfChunk.uid)
      doTest(results, expected)
    }

    it("should read empty result in uid-range") {
      val firstUid = Uid("0x100")
      val untilUid = Uid("0x200")
      val range = UidRange(firstUid, untilUid)
      val results = Map(
        chunkQuery(firstUid.before) -> emptyChunkResult
      )

      doTest(results, Seq.empty, Some(range))
    }

    it("should read empty chunk in uid-range") {
      val firstUid = Uid("0x100")
      val untilUid = Uid("0x200")
      val range = UidRange(firstUid, untilUid)
      val results = Map(
        chunkQuery(firstUid.before) -> firstFullChunkResult,
        chunkQuery(lastUidOfFirstFullChunk) -> emptyChunkResult
      )

      val expected = expecteds.filter(_.getLong(0) <= lastUidOfFirstFullChunk.uid)
      doTest(results, expected, Some(range))
    }

    it("should read chopped first chunk in uid-range") {
      val firstUid = Uid("0x100")
      val untilUid = lastUidOfFirstFullChunk
      val range = UidRange(firstUid, untilUid)
      val results = Map(
        chunkQuery(firstUid.before) -> firstFullChunkResult
      )

      val expected = expecteds.filter(_.getLong(0) <= uidBeforeLastUidOfFirstFullChunk.uid)
      doTest(results, expected, Some(range))
    }

    it("should read chopped second chunk in uid-range") {
      val firstUid = Uid("0x100")
      val untilUid = lastUidOfSecondHalfChunk
      val range = UidRange(firstUid, untilUid)
      val results = Map(
        chunkQuery(firstUid.before) -> firstFullChunkResult,
        chunkQuery(lastUidOfFirstFullChunk) -> secondHalfChunkResult
      )

      val expected = expecteds.filter(_.getLong(0) <= uidBeforeLastUidOfSecondHalfChunk.uid)
      doTest(results, expected, Some(range))
    }

    it("should read all chunks in uid-range") {
      val firstUid = Uid("0x100")
      val untilUid = Uid("0x200")
      val range = UidRange(firstUid, untilUid)
      val results = Map(
        chunkQuery(firstUid.before) -> firstFullChunkResult,
        chunkQuery(lastUidOfFirstFullChunk) -> secondHalfChunkResult
      )

      val expected = expecteds.filter(_.getLong(0) <= lastUidOfSecondHalfChunk.uid)
      doTest(results, expected, Some(range))
    }

    it("should read single chunks with uid") {
      val uids = Uids(Set(Uid("0x124")))
      val results = Map(
        chunkQueryUids(uids.uids, 2, Uid(0)) -> x124ChunkResult
      )

      val expected = expecteds.filter(_.getLong(0) == 0x124)
      doTest(results, expected, None, Some(uids), size = 2)
    }

    it("should read all chunks with uids") {
      val uids = Uids(Set(Uid("0x124"), Uid("0x125"), Uid("0x135")))
      val results = Map(
        chunkQueryUids(uids.uids, 2, Uid("0x0")) -> x124x125ChunkResult,
        chunkQueryUids(uids.uids, 2, Uid("0x125")) -> x135ChunkResult,
      )

      val expected = expecteds.filter(r => Set(0x124, 0x125, 0x135).contains(r.getLong(0).toInt))
      doTest(results, expected, None, Some(uids), size = 2)
    }

    it("should read all chunks with some unused uids") {
      val uids = Uids(Set(Uid("0x124"), Uid("0x125"), Uid("0x126"), Uid("0x127"), Uid("0x132"), Uid("0x135"), Uid("0x140"), Uid("0x141"), Uid("0x142")))
      val results = Map(
        chunkQueryUids(uids.uids, 2, Uid("0x0")) -> x124x125ChunkResult,
        chunkQueryUids(uids.uids, 2, Uid("0x125")) -> secondHalfChunkResult,
        chunkQueryUids(uids.uids, 2, Uid("0x135")) -> emptyChunkResult,
      )

      val expected = expecteds.filter(r => Set(0x124, 0x125, 0x132, 0x135).contains(r.getLong(0).toInt))
      doTest(results, expected, None, Some(uids), size = 2)
    }

    def doTest(results: Map[String, String], expected: Seq[InternalRow], uidRange: Option[UidRange] = None, uids: Option[Uids] = None, size: Int = 3): Unit = {
      val targets = Seq(Target("localhost:8090"))
      val predicates = Seq(Predicate("prop", "string"), Predicate("edge", "uid")).map(p => p.predicateName -> p).toMap

      val executor = new JsonGraphQlExecutor {
        override def query(query: GraphQl): Json =
          results.get(query.string).map(Json)
            .getOrElse(fail(s"unexpected query:\n${query.string}\n\nexpected queries:\n${results.keys.mkString("\n")}"))
      }

      val executionProvider = new ExecutorProvider {
        override def getExecutor(partition: Partition): JsonGraphQlExecutor = executor
      }

      val rowEncoder = StringTripleEncoder(predicates)
      val model = TestModel(executionProvider, rowEncoder, size)
      val partition = Partition(targets, Set(Has(predicates.values.toSet)) ++ uidRange.map(Set(_)).getOrElse(Set.empty) ++ uids.map(Set(_)).getOrElse(Set.empty)).getAll

      val rows = model.modelPartition(partition).toSeq
      assert(rows === expected)
    }

    it("should filter array") {
      val array = getChunk((1 to 10).map(id => Uid(id * 7)))
      assert(GraphTableModel.filter(array, Uid(100)) === getChunk((1 to 10).map(id => Uid(id * 7))))
      assert(GraphTableModel.filter(array, Uid(71)) === getChunk((1 to 10).map(id => Uid(id * 7))))
      assert(GraphTableModel.filter(array, Uid(70)) === getChunk((1 to 9).map(id => Uid(id * 7))))
      assert(GraphTableModel.filter(array, Uid(15)) === getChunk((1 to 2).map(id => Uid(id * 7))))
      assert(GraphTableModel.filter(array, Uid(14)) === getChunk((1 to 1).map(id => Uid(id * 7))))
      assert(GraphTableModel.filter(array, Uid(7)) === new JsonArray())
      assert(GraphTableModel.filter(array, Uid(0)) === new JsonArray())
    }

  }

}

case class TestModel(execution: ExecutorProvider,
                     encoder: JsonNodeInternalRowEncoder,
                     chunkSize: Int,
                     metrics: PartitionMetrics = NoPartitionMetrics())
  extends GraphTableModel {
  override def withMetrics(metrics: PartitionMetrics): TestModel = copy(metrics = metrics)
}
