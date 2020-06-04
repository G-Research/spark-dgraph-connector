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
      val predicates = Set(
        Predicate("prop1", "string"),
        Predicate("prop2", "long"),
        Predicate("edge1", "uid"),
        Predicate("edge2", "uid")
      )
      val query = Query.forAllPropertiesAndEdges("result", Some(predicates))
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
      val predicates = Set.empty[Predicate]
      val query = Query.forAllPropertiesAndEdges("result", Some(predicates))
      assert(query ===
        """{
          |  result (func: has(dgraph.type)) @filter(eq(true, false)) {
          |    uid
          |  }
          |}""".stripMargin)
    }

    it("should provide query for all properties and edges when no schema given") {
      val query = Query.forAllPropertiesAndEdges("result", None)
      assert(query ===
        """{
          |  result (func: has(dgraph.type))  {
          |    uid
          |  }
          |}""".stripMargin)
    }

  }
}
