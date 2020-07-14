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
    val hasProp: Set[Operator] = Set(Has(prop))
    val hasEdge: Set[Operator] = Set(Has(edge))

    val predicates = Set(
      Predicate("prop1", "string"),
      Predicate("prop2", "int"),
      Predicate("edge1", "uid"),
      Predicate("edge2", "uid")
    )
    val hasPredicates: Set[Operator] = Set(Has(predicates))
    val hasNoPredicates: Set[Operator] = Set(Has(Set.empty, Set.empty))

    val values: Set[Operator] = Set(IsIn("prop1", Set[Any]("value")), IsIn("edge2", Set[Any](1L)))
    val multiPredValues: Set[Operator] = Set(IsIn(Set("prop1", "prop2"), Set[Any]("value")), IsIn(Set("edge1", "edge2"), Set[Any](1L)))
    val multiValues: Set[Operator] = Set(IsIn("prop1", Set[Any]("one", "two")), IsIn("edge2", Set[Any](1L, 2L)))

    it("should provide query for one property") {
      val query = PartitionQuery("result", hasProp)
      assert(query.forChunk(None).string ===
        """{
          |  pred1 as var(func: has(<prop>))
          |
          |  result (func: uid(pred1)) {
          |    uid
          |    <prop>
          |  }
          |}""".stripMargin)
    }

    it("should provide query for one edge") {
      val query = PartitionQuery("result", hasEdge)
      assert(query.forChunk(None).string ===
        """{
          |  pred1 as var(func: has(<edge>))
          |
          |  result (func: uid(pred1)) {
          |    uid
          |    <edge> { uid }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for some predicates") {
      val query = PartitionQuery("result", hasPredicates)
      assert(query.forChunk(None).string ===
        """{
          |  pred1 as var(func: has(<edge1>))
          |  pred2 as var(func: has(<edge2>))
          |  pred3 as var(func: has(<prop1>))
          |  pred4 as var(func: has(<prop2>))
          |
          |  result (func: uid(pred1,pred2,pred3,pred4)) {
          |    uid
          |    <edge1> { uid }
          |    <edge2> { uid }
          |    <prop1>
          |    <prop2>
          |  }
          |}""".stripMargin)
    }

    it("should provide query for predicates with chunk") {
      val chunk = Chunk(Uid("0x123"), 10)
      val query = PartitionQuery("result", hasPredicates)
      assert(query.forChunk(Some(chunk)).string ===
        """{
          |  pred1 as var(func: has(<edge1>), first: 10, after: 0x123)
          |  pred2 as var(func: has(<edge2>), first: 10, after: 0x123)
          |  pred3 as var(func: has(<prop1>), first: 10, after: 0x123)
          |  pred4 as var(func: has(<prop2>), first: 10, after: 0x123)
          |
          |  result (func: uid(pred1,pred2,pred3,pred4), first: 10, after: 0x123) {
          |    uid
          |    <edge1> { uid }
          |    <edge2> { uid }
          |    <prop1>
          |    <prop2>
          |  }
          |}""".stripMargin)
    }

    it("should provide query for predicates with value") {
      val query = PartitionQuery("result", hasPredicates ++ values)
      assert(query.forChunk(None).string ===
        """{
          |  pred1 as var(func: has(<edge1>))
          |  pred2 as var(func: has(<edge2>)) @filter(uid_in(<edge2>, 0x1))
          |  pred3 as var(func: has(<prop1>)) @filter(eq(<prop1>, "value"))
          |  pred4 as var(func: has(<prop2>))
          |
          |  result (func: uid(pred1,pred2,pred3,pred4)) {
          |    uid
          |    <edge1> { uid }
          |    <edge2> { uid } @filter(uid(0x1))
          |    <prop1> @filter(eq(<prop1>, "value"))
          |    <prop2>
          |  }
          |}""".stripMargin)
    }

    it("should provide query for predicates with same value") {
      val query = PartitionQuery("result", hasPredicates ++ multiPredValues)
      assert(query.forChunk(None).string ===
        """{
          |  pred1 as var(func: has(<edge1>)) @filter(uid_in(<edge1>, 0x1))
          |  pred2 as var(func: has(<edge2>)) @filter(uid_in(<edge2>, 0x1))
          |  pred3 as var(func: has(<prop1>)) @filter(eq(<prop1>, "value"))
          |  pred4 as var(func: has(<prop2>)) @filter(eq(<prop2>, "value"))
          |
          |  result (func: uid(pred1,pred2,pred3,pred4)) {
          |    uid
          |    <edge1> { uid } @filter(uid(0x1))
          |    <edge2> { uid } @filter(uid(0x1))
          |    <prop1> @filter(eq(<prop1>, "value"))
          |    <prop2> @filter(eq(<prop2>, "value"))
          |  }
          |}""".stripMargin)
    }

    it("should provide query for predicates with value and chunk") {
      val chunk = Chunk(Uid("0x123"), 10)
      val query = PartitionQuery("result", hasPredicates ++ values)
      assert(query.forChunk(Some(chunk)).string ===
        """{
          |  pred1 as var(func: has(<edge1>), first: 10, after: 0x123)
          |  pred2 as var(func: has(<edge2>), first: 10, after: 0x123) @filter(uid_in(<edge2>, 0x1))
          |  pred3 as var(func: has(<prop1>), first: 10, after: 0x123) @filter(eq(<prop1>, "value"))
          |  pred4 as var(func: has(<prop2>), first: 10, after: 0x123)
          |
          |  result (func: uid(pred1,pred2,pred3,pred4), first: 10, after: 0x123) {
          |    uid
          |    <edge1> { uid }
          |    <edge2> { uid } @filter(uid(0x1))
          |    <prop1> @filter(eq(<prop1>, "value"))
          |    <prop2>
          |  }
          |}""".stripMargin)
    }

    it("should provide query for explicitly no properties and edges") {
      val query = PartitionQuery("result", hasNoPredicates)
      assert(query.forChunk(None).string ===
        """{
          |  result (func: uid()) {
          |    uid
          |  }
          |}""".stripMargin)
    }

    it("should use empty predicate queries for empty predicates") {
      val query = PartitionQuery("result", hasNoPredicates)
      assert(query.getPredicateQueries(None) === Map.empty)
    }

    it("should use single predicate query for for single predicate") {
      Seq(prop, edge).foreach { preds =>
        val query = PartitionQuery("result", Set(Has(preds)))
        assert(query.getPredicateQueries(None) === Map("pred1" -> s"""pred1 as var(func: has(<${preds.head.predicateName}>))"""))
      }
    }

    it("should use multiple predicate queries for multiple predicates") {
      val query = PartitionQuery("result", hasPredicates)
      assert(query.getPredicateQueries(None) === Map(
        "pred1" -> "pred1 as var(func: has(<edge1>))",
        "pred2" -> "pred2 as var(func: has(<edge2>))",
        "pred3" -> "pred3 as var(func: has(<prop1>))",
        "pred4" -> "pred4 as var(func: has(<prop2>))",
      ))
    }

    it("should filter values") {
      val query = PartitionQuery("result", hasPredicates ++ multiValues)
      assert(query.getPredicateQueries(None) === Map(
        "pred1" -> "pred1 as var(func: has(<edge1>))",
        "pred2" -> "pred2 as var(func: has(<edge2>)) @filter(uid_in(<edge2>, 0x1) OR uid_in(<edge2>, 0x2))",
        "pred3" -> "pred3 as var(func: has(<prop1>)) @filter(eq(<prop1>, \"one\") OR eq(<prop1>, \"two\"))",
        "pred4" -> "pred4 as var(func: has(<prop2>))",
      ))
    }

    it("should filter values and chunk predicate queries") {
      val chunk = Chunk(Uid("0x123"), 10)
      val query = PartitionQuery("result", hasPredicates ++ values)
      assert(query.getPredicateQueries(Some(chunk)) === Map(
        "pred1" -> "pred1 as var(func: has(<edge1>), first: 10, after: 0x123)",
        "pred2" -> "pred2 as var(func: has(<edge2>), first: 10, after: 0x123) @filter(uid_in(<edge2>, 0x1))",
        "pred3" -> "pred3 as var(func: has(<prop1>), first: 10, after: 0x123) @filter(eq(<prop1>, \"value\"))",
        "pred4" -> "pred4 as var(func: has(<prop2>), first: 10, after: 0x123)",
      ))
    }

    it("should filter multiple values") {
      val query = PartitionQuery("result", hasPredicates ++ multiValues)
      assert(query.getPredicateQueries(None) === Map(
        "pred1" -> "pred1 as var(func: has(<edge1>))",
        "pred2" -> "pred2 as var(func: has(<edge2>)) @filter(uid_in(<edge2>, 0x1) OR uid_in(<edge2>, 0x2))",
        "pred3" -> "pred3 as var(func: has(<prop1>)) @filter(eq(<prop1>, \"one\") OR eq(<prop1>, \"two\"))",
        "pred4" -> "pred4 as var(func: has(<prop2>))",
      ))
    }

    it("should filter multiple values and chunk predicate queries") {
      val chunk = Chunk(Uid("0x123"), 10)
      val query = PartitionQuery("result", hasPredicates ++ multiValues)
      assert(query.getPredicateQueries(Some(chunk)) === Map(
        "pred1" -> "pred1 as var(func: has(<edge1>), first: 10, after: 0x123)",
        "pred2" -> "pred2 as var(func: has(<edge2>), first: 10, after: 0x123) @filter(uid_in(<edge2>, 0x1) OR uid_in(<edge2>, 0x2))",
        "pred3" -> "pred3 as var(func: has(<prop1>), first: 10, after: 0x123) @filter(eq(<prop1>, \"one\") OR eq(<prop1>, \"two\"))",
        "pred4" -> "pred4 as var(func: has(<prop2>), first: 10, after: 0x123)",
      ))
    }

    it("should chunk predicate queries") {
      val query = PartitionQuery("result", hasPredicates)
      assert(query.getPredicateQueries(Some(Chunk(Uid("0x123"), 10))) === Map(
        "pred1" -> "pred1 as var(func: has(<edge1>), first: 10, after: 0x123)",
        "pred2" -> "pred2 as var(func: has(<edge2>), first: 10, after: 0x123)",
        "pred3" -> "pred3 as var(func: has(<prop1>), first: 10, after: 0x123)",
        "pred4" -> "pred4 as var(func: has(<prop2>), first: 10, after: 0x123)",
      ))
    }

    it("should have empty predicate paths for empty predicates set") {
      val query = PartitionQuery("result", hasNoPredicates)
      assert(query.predicatePaths === Seq.empty)
    }

    it("should have predicate paths for given predicates set") {
      val query = PartitionQuery("result", hasPredicates)
      assert(query.predicatePaths === Seq(
        "<edge1> { uid }",
        "<edge2> { uid }",
        "<prop1>",
        "<prop2>",
      ))
    }

    it("should have predicate paths for given predicates set with value") {
      val query = PartitionQuery("result", hasPredicates ++ values)
      assert(query.predicatePaths === Seq(
        "<edge1> { uid }",
        "<edge2> { uid } @filter(uid(0x1))",
        "<prop1> @filter(eq(<prop1>, \"value\"))",
        "<prop2>",
      ))
    }

    it("should have predicate paths for given predicates set with values") {
      val query = PartitionQuery("result", hasPredicates ++ multiValues)
      assert(query.predicatePaths === Seq(
        "<edge1> { uid }",
        "<edge2> { uid } @filter(uid(0x1, 0x2))",
        "<prop1> @filter(eq(<prop1>, \"one\") OR eq(<prop1>, \"two\"))",
        "<prop2>",
      ))
    }

  }
}
