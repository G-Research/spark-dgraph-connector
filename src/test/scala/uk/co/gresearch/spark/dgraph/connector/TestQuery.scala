package uk.co.gresearch.spark.dgraph.connector

import org.scalatest.FunSpec

class TestQuery extends FunSpec {

  describe("Query") {

    it("should provide query for all properties") {
      val query = Query.forAllProperties("result", None)
      assert(query ===
        """{
          |  result (func: has(dgraph.type)) {
          |    uid
          |    expand(_all_)
          |  }
          |}""".stripMargin)
    }

    it("should provide query for all properties with uid range") {
      val uids = UidRange(1000, 500)
      val query = Query.forAllProperties("result", Some(uids))
      assert(query ===
        """{
          |  result (func: has(dgraph.type), first: 500, offset: 1000) {
          |    uid
          |    expand(_all_)
          |  }
          |}""".stripMargin)
    }

    it("should provide query for all properties and edges") {
      val query = Query.forAllPropertiesAndEdges("result", None)
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

    it("should provide query for all properties and edges with uid range") {
      val uids = UidRange(1000, 500)
      val query = Query.forAllPropertiesAndEdges("result", Some(uids))
      assert(query ===
        """{
          |  result (func: has(dgraph.type), first: 500, offset: 1000) {
          |    uid
          |    expand(_all_) {
          |      uid
          |    }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for properties and edges in given schema") {
      val predicates = Set(
        Predicate("prop1", "string"),
        Predicate("prop2", "long"),
        Predicate("edge1", "uid"),
        Predicate("edge2", "uid")
      )
      val query = Query.forPropertiesAndEdges("result", predicates, None)
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

    it("should provide query for properties and edges in given schema and uid range") {
      val predicates = Set(
        Predicate("prop1", "string"),
        Predicate("prop2", "long"),
        Predicate("edge1", "uid"),
        Predicate("edge2", "uid")
      )
      val uids = UidRange(1000, 500)
      val query = Query.forPropertiesAndEdges("result", predicates, Some(uids))
      assert(query ===
        """{
          |  result (func: has(dgraph.type), first: 500, offset: 1000) @filter(has(prop1) OR has(prop2) OR has(edge1) OR has(edge2)) {
          |    uid
          |    prop1
          |    prop2
          |    edge1 { uid }
          |    edge2 { uid }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for properties and edges in given empty schema") {
      val predicates = Set.empty[Predicate]
      val query = Query.forPropertiesAndEdges("result", predicates, None)
      assert(query ===
        """{
          |  result (func: has(dgraph.type)) @filter(eq(true, false)) {
          |    uid
          |  }
          |}""".stripMargin)
    }

  }
}
