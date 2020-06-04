package uk.co.gresearch.spark.dgraph.connector

import org.scalatest.FunSpec

class TestQuery extends FunSpec {

  describe("Query") {

    it("should provide query for all properties and edges") {
      val query = Query.forAllPropertiesAndEdges("result")
      assert(query ===
        """{
          |  result (func: has(dgraph.type)) {
          |    uid
          |    expand(_all_) {
          |      uid
          |    }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for all properties and edges in given schema") {
      val schema = Schema(Map("prop1" -> "string", "prop2" -> "long", "edge1" -> "uid", "edge2" -> "uid"))
      val query = Query.forAllPropertiesAndEdges("result", schema)
      assert(query ===
        """{
          |  result (func: has(dgraph.type)) @filter(has(prop1) OR has(prop2) OR has(edge1) OR has(edge2)) {
          |    uid
          |    prop1
          |    prop2
          |    edge1 { uid }
          |    edge2 { uid }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for all properties and edges in given empty schema") {
      val schema = Schema(Map())
      val query = Query.forAllPropertiesAndEdges("result", schema)
      assert(query ===
        """{
          |  result (func: has(dgraph.type)) @filter(eq(true, false) {
          |    uid
          |  }
          |}""".stripMargin)
    }

  }
}
