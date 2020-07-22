package uk.co.gresearch.spark.dgraph.connector.model

import com.google.gson.JsonArray
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.encoder.{JsonNodeInternalRowEncoder, StringTripleEncoder}
import uk.co.gresearch.spark.dgraph.connector.executor.{ExecutorProvider, JsonGraphQlExecutor}
import uk.co.gresearch.spark.dgraph.connector.model.TestChunkIterator.getChunk
import uk.co.gresearch.spark.dgraph.connector.{GraphQl, Has, Json, Partition, Predicate, Target, Uid, UidRange}


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

    def doTest(results: Map[String, String], expected: Seq[InternalRow], uids: Option[UidRange] = None, size: Int = 3): Unit = {
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
      val partition = Partition(targets, Set(Has(predicates.values.toSet)) ++ uids.map(Set(_)).getOrElse(Set.empty), model)

      val rows = model.modelPartition(partition).toSeq
      assert(rows === expected)
    }

    it("should filter array") {
      val array = getChunk((1 to 10).map(id => Uid(id * 7)))
      println(array)
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
                     chunkSize: Int)
  extends GraphTableModel {
}
