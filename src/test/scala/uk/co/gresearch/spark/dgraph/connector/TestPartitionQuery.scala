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

    val uid = Set[Operator](Uids(Set(Uid(1))))
    val uids = Set[Operator](Uids(Set(Uid(1), Uid(2))))

    val propName = "prop"
    val edgeName = "edge"
    val prop = Set(Predicate(propName, "string"))
    val edge = Set(Predicate(edgeName, "uid"))
    val hasProp: Set[Operator] = Set(Has(prop))
    val hasEdge: Set[Operator] = Set(Has(edge))
    val hasPropAndEdge: Set[Operator] = hasProp ++ hasEdge

    val prop1Name = "prop1"
    val prop2Name = "prop2"
    val predicates = Set(
      Predicate(prop1Name, "string"),
      Predicate(prop2Name, "int"),
      Predicate("edge1", "uid"),
      Predicate("edge2", "uid")
    )
    val hasPredicates: Set[Operator] = Set(Has(predicates))
    val hasNoPredicates: Set[Operator] = Set(Has(Set.empty, Set.empty))

    val filters: Set[Operator] = Set(IsIn("prop1", Set[Any]("value")), IsIn("edge2", Set[Any](1L)))
    val multiPredFilters: Set[Operator] = Set(IsIn(Set("prop1", "prop2"), Set[Any]("value")), IsIn(Set("edge1", "edge2"), Set[Any](1L)))
    val multiValueFilters: Set[Operator] = Set(IsIn("prop1", Set[Any]("one", "two")), IsIn("edge2", Set[Any](1L, 2L)))
    val multiFilters: Set[Operator] = Set(GreaterOrEqual("prop1", 1), LessThan("prop1", 2))

    val propFilters: Set[Operator] = Set(IsIn("prop", Set[Any]("one", "two")))
    val edgeFilters: Set[Operator] = Set(IsIn("edge", Set[Any](1L, 2L)))

    val predicateValueOperators = Seq(
      LessThan(propName, 1),
      LessOrEqual(propName, 1),
      GreaterOrEqual(propName, 1),
      GreaterThan(propName, 1),
    )
    val predicatesValueOperators = Seq(
      LessThan(Set(prop1Name, prop2Name), 1),
      LessOrEqual(Set(prop1Name, prop2Name), 1),
      GreaterOrEqual(Set(prop1Name, prop2Name), 1),
      GreaterThan(Set(prop1Name, prop2Name), 1),
    )

    it("should provide query with uid") {
      val query = PartitionQuery("result", uid)
      assert(query.forChunk(None).string ===
        """{
          |  result (func: uid(0x1)) {
          |    uid
          |  }
          |}""".stripMargin)
    }

    it("should provide query with uids") {
      val query = PartitionQuery("result", uids)
      assert(query.forChunk(None).string ===
        """{
          |  result (func: uid(0x1,0x2)) {
          |    uid
          |  }
          |}""".stripMargin)
    }

    it("should provide query with uids and has") {
      val query = PartitionQuery("result", uids ++ hasPredicates)
      assert(query.forChunk(None).string ===
        """{
          |  result (func: uid(0x1,0x2)) {
          |    uid
          |    <edge1> { uid }
          |    <edge2> { uid }
          |    <prop1>
          |    <prop2>
          |  }
          |}""".stripMargin)
    }

    it("should provide query for explicitly no predicates") {
      val query = PartitionQuery("result", hasNoPredicates)
      assert(query.forChunk(None).string ===
        """{
          |  result (func: uid()) {
          |    uid
          |  }
          |}""".stripMargin)
    }

    it("should provide query for multi predicates") {
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

    it("should provide query for multiple predicates") {
      val query = PartitionQuery("result", hasPropAndEdge ++ hasPredicates)
      assert(query.forChunk(None).string ===
        """{
          |  pred1 as var(func: has(<edge>))
          |  pred2 as var(func: has(<edge1>))
          |  pred3 as var(func: has(<edge2>))
          |  pred4 as var(func: has(<prop>))
          |  pred5 as var(func: has(<prop1>))
          |  pred6 as var(func: has(<prop2>))
          |
          |  result (func: uid(pred1,pred2,pred3,pred4,pred5,pred6)) @filter(has(<edge>) AND (has(<edge1>) OR has(<edge2>) OR has(<prop1>) OR has(<prop2>)) AND has(<prop>)) {
          |    uid
          |    <edge> { uid }
          |    <edge1> { uid }
          |    <edge2> { uid }
          |    <prop>
          |    <prop1>
          |    <prop2>
          |  }
          |}""".stripMargin)
    }

    it("should provide query with other gets than has") {
      val query = PartitionQuery("result", hasPredicates ++ Set(Get(Set(propName), Set(edgeName))))
      assert(query.forChunk(None).string ===
        """{
          |  pred1 as var(func: has(<edge1>))
          |  pred2 as var(func: has(<edge2>))
          |  pred3 as var(func: has(<prop1>))
          |  pred4 as var(func: has(<prop2>))
          |
          |  result (func: uid(pred1,pred2,pred3,pred4)) {
          |    uid
          |    <edge> { uid }
          |    <prop>
          |  }
          |}""".stripMargin)
    }

    it("should provide query chunk and filter for multiple values") {
      val chunk = Chunk(Uid("0x123"), 10)
      val query = PartitionQuery("result", hasPredicates ++ multiValueFilters)
      assert(query.forChunk(Some(chunk)).string ===
        """{
          |  pred1 as var(func: has(<edge1>), first: 10, after: 0x123)
          |  pred2 as var(func: has(<edge2>), first: 10, after: 0x123) @filter(uid_in(<edge2>, 0x1) OR uid_in(<edge2>, 0x2))
          |  pred3 as var(func: has(<prop1>), first: 10, after: 0x123) @filter(eq(<prop1>, "one") OR eq(<prop1>, "two"))
          |  pred4 as var(func: has(<prop2>), first: 10, after: 0x123)
          |
          |  result (func: uid(pred1,pred2,pred3,pred4), first: 10, after: 0x123) {
          |    uid
          |    <edge1> { uid }
          |    <edge2> { uid } @filter(uid(0x1, 0x2))
          |    <prop1> @filter(eq(<prop1>, "one") OR eq(<prop1>, "two"))
          |    <prop2>
          |  }
          |}""".stripMargin)
    }

    describe("getPredicateQueries") {

      it("should be empty for empty predicates") {
        val query = PartitionQuery("result", hasNoPredicates)
        assert(query.getPredicateQueries(None) === Map.empty)
      }

      it("should work with single predicate") {
        Seq(prop, edge).foreach { preds =>
          val query = PartitionQuery("result", Set(Has(preds)))
          assert(query.getPredicateQueries(None) === Map("pred1" -> s"""pred1 as var(func: has(<${preds.head.predicateName}>))"""))
        }
      }

      it("should work with multi predicates") {
        val query = PartitionQuery("result", hasPredicates)
        assert(query.getPredicateQueries(None) === Map(
          "pred1" -> "pred1 as var(func: has(<edge1>))",
          "pred2" -> "pred2 as var(func: has(<edge2>))",
          "pred3" -> "pred3 as var(func: has(<prop1>))",
          "pred4" -> "pred4 as var(func: has(<prop2>))",
        ))
      }

      it("should work with multiple predicates") {
        val query = PartitionQuery("result", hasPropAndEdge)
        assert(query.getPredicateQueries(None) === Map(
          "pred1" -> "pred1 as var(func: has(<edge>))",
          "pred2" -> "pred2 as var(func: has(<prop>))",
        ))
      }

      it("should filter values") {
        val query = PartitionQuery("result", hasPredicates ++ filters)
        assert(query.getPredicateQueries(None) === Map(
          "pred1" -> "pred1 as var(func: has(<edge1>))",
          "pred2" -> "pred2 as var(func: has(<edge2>)) @filter(uid_in(<edge2>, 0x1))",
          "pred3" -> "pred3 as var(func: has(<prop1>)) @filter(eq(<prop1>, \"value\"))",
          "pred4" -> "pred4 as var(func: has(<prop2>))",
        ))
      }

      it("should filter values for multiple predicates") {
        val query = PartitionQuery("result", hasPredicates ++ multiPredFilters)
        assert(query.getPredicateQueries(None) === Map(
          "pred1" -> "pred1 as var(func: has(<edge1>)) @filter(uid_in(<edge1>, 0x1))",
          "pred2" -> "pred2 as var(func: has(<edge2>)) @filter(uid_in(<edge2>, 0x1))",
          "pred3" -> "pred3 as var(func: has(<prop1>)) @filter(eq(<prop1>, \"value\"))",
          "pred4" -> "pred4 as var(func: has(<prop2>)) @filter(eq(<prop2>, \"value\"))",
        ))
      }

      it("should filter multiple values") {
        val query = PartitionQuery("result", hasPredicates ++ multiValueFilters)
        assert(query.getPredicateQueries(None) === Map(
          "pred1" -> "pred1 as var(func: has(<edge1>))",
          "pred2" -> "pred2 as var(func: has(<edge2>)) @filter(uid_in(<edge2>, 0x1) OR uid_in(<edge2>, 0x2))",
          "pred3" -> "pred3 as var(func: has(<prop1>)) @filter(eq(<prop1>, \"one\") OR eq(<prop1>, \"two\"))",
          "pred4" -> "pred4 as var(func: has(<prop2>))",
        ))
      }

      it("should filter multiple values per predicate") {
        val query = PartitionQuery("result", hasPredicates ++ multiFilters)
        assert(query.getPredicateQueries(None) === Map(
          "pred1" -> "pred1 as var(func: has(<edge1>))",
          "pred2" -> "pred2 as var(func: has(<edge2>))",
          "pred3" -> "pred3 as var(func: has(<prop1>)) @filter(ge(<prop1>, \"1\") AND lt(<prop1>, \"2\"))",
          "pred4" -> "pred4 as var(func: has(<prop2>))",
        ))
      }

      predicateValueOperators.foreach { op =>
        it(s"should support predicate value operator ${op.filter} with single property") {
          val query = PartitionQuery("result", hasProp ++ Set(op))
          assert(query.getPredicateQueries(None) === Map(
            "pred1" -> s"""pred1 as var(func: has(<${op.predicates.head}>)) @filter(${op.filter}(<${op.predicates.head}>, "${op.value}"))"""
          ))
        }
      }

      predicatesValueOperators.foreach { op =>
        it(s"should support predicate value operator ${op.filter} with multiple properties") {
          val query = PartitionQuery("result", hasPredicates ++ Set(op))
          assert(query.getPredicateQueries(None) === Map(
            "pred1" -> "pred1 as var(func: has(<edge1>))",
            "pred2" -> "pred2 as var(func: has(<edge2>))",
            "pred3" -> s"""pred3 as var(func: has(<${op.predicates.head}>)) @filter(${op.filter}(<${op.predicates.head}>, "${op.value}"))""",
            "pred4" -> s"""pred4 as var(func: has(<${op.predicates.drop(1).head}>)) @filter(${op.filter}(<${op.predicates.drop(1).head}>, "${op.value}"))"""
          ))
        }
      }

      it("should chunk") {
        val chunk = Chunk(Uid("0x123"), 10)
        val query = PartitionQuery("result", hasPredicates)
        assert(query.getPredicateQueries(Some(chunk)) === Map(
          "pred1" -> "pred1 as var(func: has(<edge1>), first: 10, after: 0x123)",
          "pred2" -> "pred2 as var(func: has(<edge2>), first: 10, after: 0x123)",
          "pred3" -> "pred3 as var(func: has(<prop1>), first: 10, after: 0x123)",
          "pred4" -> "pred4 as var(func: has(<prop2>), first: 10, after: 0x123)",
        ))
      }

      it("should chunk and filter") {
        val chunk = Chunk(Uid("0x123"), 10)
        val query = PartitionQuery("result", hasPredicates ++ multiValueFilters)
        assert(query.getPredicateQueries(Some(chunk)) === Map(
          "pred1" -> "pred1 as var(func: has(<edge1>), first: 10, after: 0x123)",
          "pred2" -> "pred2 as var(func: has(<edge2>), first: 10, after: 0x123) @filter(uid_in(<edge2>, 0x1) OR uid_in(<edge2>, 0x2))",
          "pred3" -> "pred3 as var(func: has(<prop1>), first: 10, after: 0x123) @filter(eq(<prop1>, \"one\") OR eq(<prop1>, \"two\"))",
          "pred4" -> "pred4 as var(func: has(<prop2>), first: 10, after: 0x123)",
        ))
      }

    }

    describe("predicatePaths") {

      it("should be empty for empty predicates set") {
        val query = PartitionQuery("result", hasNoPredicates)
        assert(query.predicatePaths === Seq.empty)
      }

      it("should work with multi predicates") {
        val query = PartitionQuery("result", hasPredicates)
        assert(query.predicatePaths === Seq(
          "<edge1> { uid }",
          "<edge2> { uid }",
          "<prop1>",
          "<prop2>",
        ))
      }

      it("should work with multiple predicates") {
        val query = PartitionQuery("result", hasPropAndEdge)
        assert(query.predicatePaths === Seq(
          "<edge> { uid }",
          "<prop>",
        ))
      }

      it("should filter values") {
        val query = PartitionQuery("result", hasPredicates ++ filters)
        assert(query.predicatePaths === Seq(
          "<edge1> { uid }",
          "<edge2> { uid } @filter(uid(0x1))",
          "<prop1> @filter(eq(<prop1>, \"value\"))",
          "<prop2>",
        ))
      }

      it("should filter multiple values") {
        val query = PartitionQuery("result", hasPredicates ++ multiValueFilters)
        assert(query.predicatePaths === Seq(
          "<edge1> { uid }",
          "<edge2> { uid } @filter(uid(0x1, 0x2))",
          "<prop1> @filter(eq(<prop1>, \"one\") OR eq(<prop1>, \"two\"))",
          "<prop2>",
        ))
      }

      it("should filter multiple values per predicate") {
        val query = PartitionQuery("result", hasPredicates ++ multiFilters)
        assert(query.predicatePaths === Seq(
          "<edge1> { uid }",
          "<edge2> { uid }",
          "<prop1> @filter(ge(<prop1>, \"1\") AND lt(<prop1>, \"2\"))",
          "<prop2>",
        ))
      }

      predicateValueOperators.foreach { op =>
        it(s"should support predicate value ${op.filter} with single predicate") {
          val query = PartitionQuery("result", hasProp ++ Set(op))
          assert(query.predicatePaths === Seq(
            s"""<${op.predicates.head}> @filter(${op.filter}(<${op.predicates.head}>, "${op.value}"))""",
          ))
        }
      }

      predicatesValueOperators.foreach { op =>
        it(s"should support predicate value ${op.filter} with multiple predicates") {
          val query = PartitionQuery("result", hasPredicates ++ Set(op))
          assert(query.predicatePaths === Seq(
            "<edge1> { uid }",
            "<edge2> { uid }",
            s"""<${op.predicates.head}> @filter(${op.filter}(<${op.predicates.head}>, "${op.value}"))""",
            s"""<${op.predicates.drop(1).head}> @filter(${op.filter}(<${op.predicates.drop(1).head}>, "${op.value}"))""",
          ))
        }
      }

    }

    describe("resultOperatorFilters") {
      it("should not filter no operator") {
        val query = PartitionQuery("result", Set.empty)
        assert(query.resultOperatorFilters === None)
      }

      it("should not filter has with single predicate") {
        val query = PartitionQuery("result", hasEdge)
        assert(query.resultOperatorFilters === None)
      }

      it("should not filter has with multiple predicates") {
        val query = PartitionQuery("result", hasPredicates)
        assert(query.resultOperatorFilters === None)
      }

      it("should not filter has with multiple predicates and filters") {
        val query = PartitionQuery("result", hasPredicates ++ filters)
        assert(query.resultOperatorFilters === None)
      }

      it("should filter multiple has operators") {
        val query = PartitionQuery("result", hasPropAndEdge)
        assert(query.resultOperatorFilters === Some("has(<edge>) AND has(<prop>)"))
      }

      it("should filter multiple has operators with filter") {
        val query = PartitionQuery("result", hasPropAndEdge ++ propFilters ++ edgeFilters)
        assert(query.resultOperatorFilters === Some("(uid_in(<edge>, 0x1) OR uid_in(<edge>, 0x2)) AND (eq(<prop>, \"one\") OR eq(<prop>, \"two\"))"))
      }
    }

  }
}
