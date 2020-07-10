/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector

import org.scalatest.FunSpec

class TestPartitionQuery extends FunSpec {

  describe("PartitionQuery") {

    val prop = Set(Predicate("prop", "string"))
    val edge = Set(Predicate("edge", "uid"))

    val predicates = Set(
      Predicate("prop1", "string"),
      Predicate("prop2", "int"),
      Predicate("edge1", "uid"),
      Predicate("edge2", "uid")
    )

    val values: Map[String, Set[Any]] = Map("pred" -> Set("IGNORED"), "prop1" -> Set("value"), "edge2" -> Set(1L))
    val multiValues: Map[String, Set[Any]] = Map("prop1" -> Set("one", "two"), "edge2" -> Set(1L, 2L))

    it("should provide query for one property") {
      val query = PartitionQuery("result", Some(prop), None)
      assert(query.forProperties(None).string ===
        """{
          |  pred1 as var(func: has(<prop>))
          |
          |  result (func: uid(pred1)) {
          |    uid
          |    <prop>
          |  }
          |}""".stripMargin)
    }

    it("should provide query for one property given edge") {
      val query = PartitionQuery("result", Some(edge), None)
      assert(query.forProperties(None).string ===
        """{
          |  pred1 as var(func: has(<edge>))
          |
          |  result (func: uid(pred1)) {
          |    uid
          |    <edge> { uid }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for one property or edge") {
      val query1 = PartitionQuery("result", Some(prop), None)
      assert(query1.forProperties(None).string ===
        """{
          |  pred1 as var(func: has(<prop>))
          |
          |  result (func: uid(pred1)) {
          |    uid
          |    <prop>
          |  }
          |}""".stripMargin)

      val query2 = PartitionQuery("result", Some(edge), None)
      assert(query2.forProperties(None).string ===
        """{
          |  pred1 as var(func: has(<edge>))
          |
          |  result (func: uid(pred1)) {
          |    uid
          |    <edge> { uid }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for all properties") {
      val query = PartitionQuery("result", None, None)
      assert(query.forProperties(None).string ===
        """{
          |  result (func: has(dgraph.type)) {
          |    uid
          |    dgraph.type
          |    expand(_all_)
          |  }
          |}""".stripMargin)
    }

    it("should provide query for some properties") {
      val query = PartitionQuery("result", Some(predicates), None)
      assert(query.forProperties(None).string ===
        """{
          |  pred1 as var(func: has(<prop1>))
          |  pred2 as var(func: has(<prop2>))
          |  pred3 as var(func: has(<edge1>))
          |  pred4 as var(func: has(<edge2>))
          |
          |  result (func: uid(pred1,pred2,pred3,pred4)) {
          |    uid
          |    <prop1>
          |    <prop2>
          |    <edge1> { uid }
          |    <edge2> { uid }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for properties with chunk") {
      val chunk = Chunk(Uid("0x123"), 10)
      val query = PartitionQuery("result", Some(prop), None)
      assert(query.forProperties(Some(chunk)).string ===
        """{
          |  pred1 as var(func: has(<prop>), first: 10, after: 0x123)
          |
          |  result (func: uid(pred1), first: 10, after: 0x123) {
          |    uid
          |    <prop>
          |  }
          |}""".stripMargin)
    }

    it("should provide query for all properties with chunk") {
      val chunk = Chunk(Uid("0x123"), 10)
      val query = PartitionQuery("result", None, None)
      assert(query.forProperties(Some(chunk)).string ===
        """{
          |  result (func: has(dgraph.type), first: 10, after: 0x123) {
          |    uid
          |    dgraph.type
          |    expand(_all_)
          |  }
          |}""".stripMargin)
    }

    it("should provide query for some properties with value and chunk") {
      val chunk = Chunk(Uid("0x123"), 10)
      val query = PartitionQuery("result", Some(predicates), Some(values))
      assert(query.forProperties(Some(chunk)).string ===
        """{
          |  pred1 as var(func: has(<prop1>), first: 10, after: 0x123) @filter(eq(<prop1>, "value"))
          |  pred2 as var(func: has(<prop2>), first: 10, after: 0x123)
          |  pred3 as var(func: has(<edge1>), first: 10, after: 0x123)
          |  pred4 as var(func: has(<edge2>), first: 10, after: 0x123) @filter(uid_in(<edge2>, 0x1))
          |
          |  result (func: uid(pred1,pred2,pred3,pred4), first: 10, after: 0x123) {
          |    uid
          |    <prop1> @filter(eq(<prop1>, "value"))
          |    <prop2>
          |    <edge1> { uid }
          |    <edge2> { uid } @filter(uid(0x1))
          |  }
          |}""".stripMargin)
    }

    it("should provide query for some properties with value") {
      val query = PartitionQuery("result", Some(predicates), Some(values))
      assert(query.forProperties(None).string ===
        """{
          |  pred1 as var(func: has(<prop1>)) @filter(eq(<prop1>, "value"))
          |  pred2 as var(func: has(<prop2>))
          |  pred3 as var(func: has(<edge1>))
          |  pred4 as var(func: has(<edge2>)) @filter(uid_in(<edge2>, 0x1))
          |
          |  result (func: uid(pred1,pred2,pred3,pred4)) {
          |    uid
          |    <prop1> @filter(eq(<prop1>, "value"))
          |    <prop2>
          |    <edge1> { uid }
          |    <edge2> { uid } @filter(uid(0x1))
          |  }
          |}""".stripMargin)
    }

    it("should provide query for some properties with values") {
      val query = PartitionQuery("result", Some(predicates), Some(multiValues))
      assert(query.forProperties(None).string ===
        """{
          |  pred1 as var(func: has(<prop1>)) @filter(eq(<prop1>, "one") OR eq(<prop1>, "two"))
          |  pred2 as var(func: has(<prop2>))
          |  pred3 as var(func: has(<edge1>))
          |  pred4 as var(func: has(<edge2>)) @filter(uid_in(<edge2>, 0x1) OR uid_in(<edge2>, 0x2))
          |
          |  result (func: uid(pred1,pred2,pred3,pred4)) {
          |    uid
          |    <prop1> @filter(eq(<prop1>, "one") OR eq(<prop1>, "two"))
          |    <prop2>
          |    <edge1> { uid }
          |    <edge2> { uid } @filter(uid(0x1, 0x2))
          |  }
          |}""".stripMargin)
    }


    it("should provide query for properties and edges with value") {
      val query = PartitionQuery("result", Some(predicates), Some(values))
      assert(query.forPropertiesAndEdges(None).string ===
        """{
          |  pred1 as var(func: has(<prop1>)) @filter(eq(<prop1>, "value"))
          |  pred2 as var(func: has(<prop2>))
          |  pred3 as var(func: has(<edge1>))
          |  pred4 as var(func: has(<edge2>)) @filter(uid_in(<edge2>, 0x1))
          |
          |  result (func: uid(pred1,pred2,pred3,pred4)) {
          |    uid
          |    <prop1> @filter(eq(<prop1>, "value"))
          |    <prop2>
          |    <edge1> { uid }
          |    <edge2> { uid } @filter(uid(0x1))
          |  }
          |}""".stripMargin)
    }

    it("should provide query for properties and edges with values") {
      val query = PartitionQuery("result", Some(predicates), Some(multiValues))
      assert(query.forPropertiesAndEdges(None).string ===
        """{
          |  pred1 as var(func: has(<prop1>)) @filter(eq(<prop1>, "one") OR eq(<prop1>, "two"))
          |  pred2 as var(func: has(<prop2>))
          |  pred3 as var(func: has(<edge1>))
          |  pred4 as var(func: has(<edge2>)) @filter(uid_in(<edge2>, 0x1) OR uid_in(<edge2>, 0x2))
          |
          |  result (func: uid(pred1,pred2,pred3,pred4)) {
          |    uid
          |    <prop1> @filter(eq(<prop1>, "one") OR eq(<prop1>, "two"))
          |    <prop2>
          |    <edge1> { uid }
          |    <edge2> { uid } @filter(uid(0x1, 0x2))
          |  }
          |}""".stripMargin)
    }

    it("should provide query for properties and edges with value and chunk") {
      val chunk = Chunk(Uid("0x123"), 10)
      val query = PartitionQuery("result", Some(predicates), Some(values))
      assert(query.forPropertiesAndEdges(Some(chunk)).string ===
        """{
          |  pred1 as var(func: has(<prop1>), first: 10, after: 0x123) @filter(eq(<prop1>, "value"))
          |  pred2 as var(func: has(<prop2>), first: 10, after: 0x123)
          |  pred3 as var(func: has(<edge1>), first: 10, after: 0x123)
          |  pred4 as var(func: has(<edge2>), first: 10, after: 0x123) @filter(uid_in(<edge2>, 0x1))
          |
          |  result (func: uid(pred1,pred2,pred3,pred4), first: 10, after: 0x123) {
          |    uid
          |    <prop1> @filter(eq(<prop1>, "value"))
          |    <prop2>
          |    <edge1> { uid }
          |    <edge2> { uid } @filter(uid(0x1))
          |  }
          |}""".stripMargin)
    }

    it("should provide query for properties and edges with chunk") {
      val chunk = Chunk(Uid("0x123"), 10)
      val query = PartitionQuery("result", Some(predicates), None)
      assert(query.forPropertiesAndEdges(Some(chunk)).string ===
        """{
          |  pred1 as var(func: has(<prop1>), first: 10, after: 0x123)
          |  pred2 as var(func: has(<prop2>), first: 10, after: 0x123)
          |  pred3 as var(func: has(<edge1>), first: 10, after: 0x123)
          |  pred4 as var(func: has(<edge2>), first: 10, after: 0x123)
          |
          |  result (func: uid(pred1,pred2,pred3,pred4), first: 10, after: 0x123) {
          |    uid
          |    <prop1>
          |    <prop2>
          |    <edge1> { uid }
          |    <edge2> { uid }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for all properties and edges with chunk") {
      val chunk = Chunk(Uid("0x123"), 10)
      val query = PartitionQuery("result", None, None)
      assert(query.forPropertiesAndEdges(Some(chunk)).string ===
        """{
          |  result (func: has(dgraph.type), first: 10, after: 0x123) {
          |    uid
          |    dgraph.type
          |    expand(_all_) {
          |      uid
          |    }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for all properties and edges") {
      val query = PartitionQuery("result", None, None)
      assert(query.forPropertiesAndEdges(None).string ===
        """{
          |  result (func: has(dgraph.type)) {
          |    uid
          |    dgraph.type
          |    expand(_all_) {
          |      uid
          |    }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for some properties and edges") {
      val query = PartitionQuery("result", Some(predicates), None)
      assert(query.forPropertiesAndEdges(None).string ===
        """{
          |  pred1 as var(func: has(<prop1>))
          |  pred2 as var(func: has(<prop2>))
          |  pred3 as var(func: has(<edge1>))
          |  pred4 as var(func: has(<edge2>))
          |
          |  result (func: uid(pred1,pred2,pred3,pred4)) {
          |    uid
          |    <prop1>
          |    <prop2>
          |    <edge1> { uid }
          |    <edge2> { uid }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for one properties or edge") {
      val query1 = PartitionQuery("result", Some(prop), None)
      assert(query1.forPropertiesAndEdges(None).string ===
        """{
          |  pred1 as var(func: has(<prop>))
          |
          |  result (func: uid(pred1)) {
          |    uid
          |    <prop>
          |  }
          |}""".stripMargin)

      val query2 = PartitionQuery("result", Some(edge), None)
      assert(query2.forPropertiesAndEdges(None).string ===
        """{
          |  pred1 as var(func: has(<edge>))
          |
          |  result (func: uid(pred1)) {
          |    uid
          |    <edge> { uid }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for explicitly no properties and edges") {
      val query = PartitionQuery("result", Some(Set.empty), None)
      assert(query.forPropertiesAndEdges(None).string ===
        """{
          |  result (func: uid()) {
          |    uid
          |  }
          |}""".stripMargin)
    }

    it("should use empty predicate queries for none predicates") {
      val query = PartitionQuery("result", None, None)
      assert(query.getPredicateQueries(None) === Map.empty)
    }

    it("should use empty predicate queries for empty predicates") {
      val query = PartitionQuery("result", Some(Set.empty), None)
      assert(query.getPredicateQueries(None) === Map.empty)
    }

    it("should use single predicate query for for single predicate") {
      Seq(prop, edge).foreach { preds =>
        val query = PartitionQuery("result", Some(preds), None)
        assert(query.getPredicateQueries(None) === Map("pred1" -> s"""pred1 as var(func: has(<${preds.head.predicateName}>))"""))
      }
    }

    it("should use multiple predicate queries for multiple predicates") {
      val query = PartitionQuery("result", Some(predicates), None)
      assert(query.getPredicateQueries(None) === Map(
        "pred1" -> "pred1 as var(func: has(<prop1>))",
        "pred2" -> "pred2 as var(func: has(<prop2>))",
        "pred3" -> "pred3 as var(func: has(<edge1>))",
        "pred4" -> "pred4 as var(func: has(<edge2>))",
      ))
    }

    it("should filter values") {
      val query = PartitionQuery("result", Some(predicates), Some(values))
      assert(query.getPredicateQueries(None) === Map(
        "pred1" -> "pred1 as var(func: has(<prop1>)) @filter(eq(<prop1>, \"value\"))",
        "pred2" -> "pred2 as var(func: has(<prop2>))",
        "pred3" -> "pred3 as var(func: has(<edge1>))",
        "pred4" -> "pred4 as var(func: has(<edge2>)) @filter(uid_in(<edge2>, 0x1))",
      ))
    }

    it("should filter values and chunk predicate queries") {
      val chunk = Chunk(Uid("0x123"), 10)
      val query = PartitionQuery("result", Some(predicates), Some(values))
      assert(query.getPredicateQueries(Some(chunk)) === Map(
        "pred1" -> "pred1 as var(func: has(<prop1>), first: 10, after: 0x123) @filter(eq(<prop1>, \"value\"))",
        "pred2" -> "pred2 as var(func: has(<prop2>), first: 10, after: 0x123)",
        "pred3" -> "pred3 as var(func: has(<edge1>), first: 10, after: 0x123)",
        "pred4" -> "pred4 as var(func: has(<edge2>), first: 10, after: 0x123) @filter(uid_in(<edge2>, 0x1))",
      ))
    }

    it("should filter multiple values") {
      val query = PartitionQuery("result", Some(predicates), Some(multiValues))
      assert(query.getPredicateQueries(None) === Map(
        "pred1" -> "pred1 as var(func: has(<prop1>)) @filter(eq(<prop1>, \"one\") OR eq(<prop1>, \"two\"))",
        "pred2" -> "pred2 as var(func: has(<prop2>))",
        "pred3" -> "pred3 as var(func: has(<edge1>))",
        "pred4" -> "pred4 as var(func: has(<edge2>)) @filter(uid_in(<edge2>, 0x1) OR uid_in(<edge2>, 0x2))",
      ))
    }

    it("should filter multiple values and chunk predicate queries") {
      val chunk = Chunk(Uid("0x123"), 10)
      val query = PartitionQuery("result", Some(predicates), Some(multiValues))
      assert(query.getPredicateQueries(Some(chunk)) === Map(
        "pred1" -> "pred1 as var(func: has(<prop1>), first: 10, after: 0x123) @filter(eq(<prop1>, \"one\") OR eq(<prop1>, \"two\"))",
        "pred2" -> "pred2 as var(func: has(<prop2>), first: 10, after: 0x123)",
        "pred3" -> "pred3 as var(func: has(<edge1>), first: 10, after: 0x123)",
        "pred4" -> "pred4 as var(func: has(<edge2>), first: 10, after: 0x123) @filter(uid_in(<edge2>, 0x1) OR uid_in(<edge2>, 0x2))",
      ))
    }

    it("should chunk predicate queries") {
      val query = PartitionQuery("result", Some(predicates), None)
      assert(query.getPredicateQueries(Some(Chunk(Uid("0x123"), 10))) === Map(
        "pred1" -> "pred1 as var(func: has(<prop1>), first: 10, after: 0x123)",
        "pred2" -> "pred2 as var(func: has(<prop2>), first: 10, after: 0x123)",
        "pred3" -> "pred3 as var(func: has(<edge1>), first: 10, after: 0x123)",
        "pred4" -> "pred4 as var(func: has(<edge2>), first: 10, after: 0x123)",
      ))
    }

    it("should have empty predicate paths for none predicates set") {
      val query = PartitionQuery("result", None, None)
      assert(query.predicatePaths === Seq.empty)
    }

    it("should have empty predicate paths for empty predicates set") {
      val query = PartitionQuery("result", Some(Set.empty), None)
      assert(query.predicatePaths === Seq.empty)
    }

    it("should have predicate paths for given predicates set") {
      val query = PartitionQuery("result", Some(predicates), None)
      assert(query.predicatePaths === Seq(
        "<prop1>",
        "<prop2>",
        "<edge1> { uid }",
        "<edge2> { uid }"
      ))
    }

    it("should have predicate paths for given predicates set with value") {
      val query = PartitionQuery("result", Some(predicates), Some(values))
      assert(query.predicatePaths === Seq(
        "<prop1> @filter(eq(<prop1>, \"value\"))",
        "<prop2>",
        "<edge1> { uid }",
        "<edge2> { uid } @filter(uid(0x1))"
      ))
    }

    it("should have predicate paths for given predicates set with values") {
      val query = PartitionQuery("result", Some(predicates), Some(multiValues))
      assert(query.predicatePaths === Seq(
        "<prop1> @filter(eq(<prop1>, \"one\") OR eq(<prop1>, \"two\"))",
        "<prop2>",
        "<edge1> { uid }",
        "<edge2> { uid } @filter(uid(0x1, 0x2))"
      ))
    }

  }
}
