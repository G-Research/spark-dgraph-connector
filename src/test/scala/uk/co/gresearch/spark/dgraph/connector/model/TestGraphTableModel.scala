package uk.co.gresearch.spark.dgraph.connector.model

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.encoder.{InternalRowEncoder, JsonNodeInternalRowEncoder, StringTripleEncoder}
import uk.co.gresearch.spark.dgraph.connector.executor.{ExecutorProvider, JsonGraphQlExecutor}
import uk.co.gresearch.spark.dgraph.connector.{GraphQl, Json, Partition, PartitionQuery, Predicate, Target, Uid, UidRange}

class TestGraphTableModel extends FunSpec {

  describe("GraphTableModel") {

    val zeroUid = Uid("0x0")

    def chunkQuery(after: Uid): String =
      s"""{
         |  pred1 as var(func: has(<prop>), first: 3, after: ${after.toHexString})
         |  pred2 as var(func: has(<edge>), first: 3, after: ${after.toHexString})
         |
         |  result (func: uid(pred1,pred2), first: 3, after: ${after.toHexString}) {
         |    uid
         |    <prop>
         |    <edge> { uid }
         |  }
         |}""".stripMargin

    val emptyChunkResult = """{"result":[]}"""

    val firstFullChunkResult =
      """{
        |  "result": [
        |    {
        |      "uid": "0x123",
        |      "prop": "str123",
        |      "edge": [{"uid": "0x123a"}]
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
        |      "prop": "str132",
        |      "edge": [{"uid": "0x132a"}]
        |    },
        |    {
        |      "uid": "0x135",
        |      "prop": "str135",
        |      "edge": [{"uid": "0x135a"}]
        |    }
        |  ]
        |}
        |""".stripMargin
    val lastUidOfSecondHalfChunk = Uid("0x135")
    val uidBeforeLastUidOfSecondHalfChunk = Uid("0x132")

    val expecteds = Seq(
      InternalRow(Uid("0x123").uid, UTF8String.fromString("prop"), UTF8String.fromString("str123"), UTF8String.fromString("string")),
      InternalRow(Uid("0x123").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x123a").toString), UTF8String.fromString("uid")),
      InternalRow(Uid("0x124").uid, UTF8String.fromString("prop"), UTF8String.fromString("str124"), UTF8String.fromString("string")),
      InternalRow(Uid("0x125").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x125a").toString), UTF8String.fromString("uid")),
      InternalRow(Uid("0x125").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x125b").toString), UTF8String.fromString("uid")),
      InternalRow(Uid("0x132").uid, UTF8String.fromString("prop"), UTF8String.fromString("str132"), UTF8String.fromString("string")),
      InternalRow(Uid("0x132").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x132a").toString), UTF8String.fromString("uid")),
      InternalRow(Uid("0x135").uid, UTF8String.fromString("prop"), UTF8String.fromString("str135"), UTF8String.fromString("string")),
      InternalRow(Uid("0x135").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x135a").toString), UTF8String.fromString("uid"))
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

      val model = new GraphTableModel {
        override val execution: ExecutorProvider = executionProvider
        override val encoder: JsonNodeInternalRowEncoder = rowEncoder
        override val chunkSize: Int = size

        override def toGraphQl(query: PartitionQuery, chunk: Option[connector.Chunk]): connector.GraphQl =
          query.forPropertiesAndEdges(chunk)

      }

      val partition = Partition(targets, Some(predicates.values.toSet), uids)

      val rows = model.modelPartition(partition).toSeq
      assert(rows === expected)
    }

  }

}
