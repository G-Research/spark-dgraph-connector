package uk.co.gresearch.spark.dgraph.connector.model

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.encoder.{InternalRowEncoder, StringTripleEncoder}
import uk.co.gresearch.spark.dgraph.connector.executor.{ExecutorProvider, JsonGraphQlExecutor}
import uk.co.gresearch.spark.dgraph.connector.{GraphQl, Json, Partition, PartitionQuery, Predicate, Target, Uid}

class TestGraphTableModel extends FunSpec {

  describe("GraphTableModel") {

    it("should read empty result") {
      val results = Map(
        """{
          |  pred1 as var(func: has(<prop>), first: 3, after: 0x0)
          |  pred2 as var(func: has(<edge>), first: 3, after: 0x0)
          |
          |  result (func: uid(pred1,pred2), first: 3, after: 0x0) {
          |    uid
          |    <prop>
          |    <edge> { uid }
          |  }
          |}""".stripMargin -> """{"result":[]}""")
      doTest(3, results, Seq.empty)
    }

    it("should read empty chunk") {
      val results = Map(
        """{
          |  pred1 as var(func: has(<prop>), first: 3, after: 0x0)
          |  pred2 as var(func: has(<edge>), first: 3, after: 0x0)
          |
          |  result (func: uid(pred1,pred2), first: 3, after: 0x0) {
          |    uid
          |    <prop>
          |    <edge> { uid }
          |  }
          |}""".stripMargin
          ->
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
            |""".stripMargin,
        """{
          |  pred1 as var(func: has(<prop>), first: 3, after: 0x125)
          |  pred2 as var(func: has(<edge>), first: 3, after: 0x125)
          |
          |  result (func: uid(pred1,pred2), first: 3, after: 0x125) {
          |    uid
          |    <prop>
          |    <edge> { uid }
          |  }
          |}""".stripMargin -> """{"result":[]}""")
      val expected = Seq(
        InternalRow(Uid("0x123").uid, UTF8String.fromString("prop"), UTF8String.fromString("str123"), UTF8String.fromString("string")),
        InternalRow(Uid("0x123").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x123a").toString), UTF8String.fromString("uid")),
        InternalRow(Uid("0x124").uid, UTF8String.fromString("prop"), UTF8String.fromString("str124"), UTF8String.fromString("string")),
        InternalRow(Uid("0x125").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x125a").toString), UTF8String.fromString("uid")),
        InternalRow(Uid("0x125").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x125b").toString), UTF8String.fromString("uid"))
      )
      doTest(3, results, expected)
    }

    it("should read all chunks") {
      val results = Map(
        """{
          |  pred1 as var(func: has(<prop>), first: 3, after: 0x0)
          |  pred2 as var(func: has(<edge>), first: 3, after: 0x0)
          |
          |  result (func: uid(pred1,pred2), first: 3, after: 0x0) {
          |    uid
          |    <prop>
          |    <edge> { uid }
          |  }
          |}""".stripMargin
          ->
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
            |""".stripMargin,
        """{
          |  pred1 as var(func: has(<prop>), first: 3, after: 0x125)
          |  pred2 as var(func: has(<edge>), first: 3, after: 0x125)
          |
          |  result (func: uid(pred1,pred2), first: 3, after: 0x125) {
          |    uid
          |    <prop>
          |    <edge> { uid }
          |  }
          |}""".stripMargin
          ->
          """{
            |  "result": [
            |    {
            |      "uid": "0x132",
            |      "prop": "str132",
            |      "edge": [{"uid": "0x132a"}]
            |    }
            |  ]
            |}
            |""".stripMargin
      )
      val expected = Seq(
        InternalRow(Uid("0x123").uid, UTF8String.fromString("prop"), UTF8String.fromString("str123"), UTF8String.fromString("string")),
        InternalRow(Uid("0x123").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x123a").toString), UTF8String.fromString("uid")),
        InternalRow(Uid("0x124").uid, UTF8String.fromString("prop"), UTF8String.fromString("str124"), UTF8String.fromString("string")),
        InternalRow(Uid("0x125").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x125a").toString), UTF8String.fromString("uid")),
        InternalRow(Uid("0x125").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x125b").toString), UTF8String.fromString("uid")),
        InternalRow(Uid("0x132").uid, UTF8String.fromString("prop"), UTF8String.fromString("str132"), UTF8String.fromString("string")),
        InternalRow(Uid("0x132").uid, UTF8String.fromString("edge"), UTF8String.fromString(Uid("0x132a").toString), UTF8String.fromString("uid"))
      )
      doTest(3, results, expected)
    }

    def doTest(size: Int, results: Map[String, String], expected: Seq[InternalRow]): Unit = {
      val targets = Seq(Target("localhost:8090"))
      val predicates = Seq(Predicate("prop", "string"), Predicate("edge", "uid")).map(p => p.predicateName -> p).toMap

      val executor = new JsonGraphQlExecutor {
        override def query(query: GraphQl): Json =
          results.get(query.string).map(Json)
            .getOrElse(fail(s"unexpected query: ${query.string}"))
      }

      val executionProvider = new ExecutorProvider {
        override def getExecutor(partition: Partition): JsonGraphQlExecutor = executor
      }

      val rowEncoder = StringTripleEncoder(predicates)

      val model = new GraphTableModel {
        override val execution: ExecutorProvider = executionProvider
        override val encoder: InternalRowEncoder = rowEncoder
        override val chunkSize: Option[Int] = Some(size)

        override def toGraphQl(query: PartitionQuery, chunk: Option[connector.Chunk]): connector.GraphQl =
          query.forPropertiesAndEdges(chunk)

      }

      val partition = Partition(targets, Some(predicates.values.toSet), None, model)

      val rows = model.readChunks(partition, size).toSeq
      assert(rows === expected)
    }

  }

}
