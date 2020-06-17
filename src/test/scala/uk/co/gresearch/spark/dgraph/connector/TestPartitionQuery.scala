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

    val predicates = Set(
      Predicate("prop1", "string"),
      Predicate("prop2", "long"),
      Predicate("edge1", "uid"),
      Predicate("edge2", "uid")
    )

    it("should provide query for all properties") {
      val query = PartitionQuery("result", None, None)
      assert(query.forProperties.string ===
        """{
          |  result (func: has(dgraph.type)) {
          |    uid
          |    dgraph.graphql.schema
          |    dgraph.type
          |    expand(_all_)
          |  }
          |}""".stripMargin)
    }

    it("should provide query for specific properties") {
      val query = PartitionQuery("result", Some(predicates), None)
      assert(query.forProperties.string ===
        """{
          |  result (func: has(dgraph.type)) @filter(has(<prop1>) OR has(<prop2>) OR has(<edge1>) OR has(<edge2>)) {
          |    uid
          |    <prop1>
          |    <prop2>
          |    <edge1> { uid }
          |    <edge2> { uid }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for all properties with uid range") {
      val uids = UidRange(1000, 500)
      val query = PartitionQuery("result", None, Some(uids))
      assert(query.forProperties.string ===
        """{
          |  result (func: has(dgraph.type), first: 500, offset: 1000) {
          |    uid
          |    dgraph.graphql.schema
          |    dgraph.type
          |    expand(_all_)
          |  }
          |}""".stripMargin)
    }

    it("should provide query for all properties and edges") {
      val query = PartitionQuery("result", None, None)
      assert(query.forPropertiesAndEdges.string ===
        """{
          |  result (func: has(dgraph.type)) {
          |    uid
          |    dgraph.graphql.schema
          |    dgraph.type
          |    expand(_all_) {
          |      uid
          |    }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for all properties and edges with uid range") {
      val uids = UidRange(1000, 500)
      val query = PartitionQuery("result", None, Some(uids))
      assert(query.forPropertiesAndEdges.string ===
        """{
          |  result (func: has(dgraph.type), first: 500, offset: 1000) {
          |    uid
          |    dgraph.graphql.schema
          |    dgraph.type
          |    expand(_all_) {
          |      uid
          |    }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for specific properties and edges") {
      val query = PartitionQuery("result", Some(predicates), None)
      assert(query.forPropertiesAndEdges.string ===
        """{
          |  result (func: has(dgraph.type)) @filter(has(<prop1>) OR has(<prop2>) OR has(<edge1>) OR has(<edge2>)) {
          |    uid
          |    <prop1>
          |    <prop2>
          |    <edge1> { uid }
          |    <edge2> { uid }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for specific properties and edges with uid range") {
      val uids = UidRange(1000, 500)
      val query = PartitionQuery("result", Some(predicates), Some(uids))
      assert(query.forPropertiesAndEdges.string ===
        """{
          |  result (func: has(dgraph.type), first: 500, offset: 1000) @filter(has(<prop1>) OR has(<prop2>) OR has(<edge1>) OR has(<edge2>)) {
          |    uid
          |    <prop1>
          |    <prop2>
          |    <edge1> { uid }
          |    <edge2> { uid }
          |  }
          |}""".stripMargin)
    }

    it("should provide query for explicitly no properties and edges") {
      val query = PartitionQuery("result", Some(Set.empty), None)
      assert(query.forPropertiesAndEdges.string ===
        """{
          |  result (func: has(dgraph.type)) @filter(eq(true, false)) {
          |    uid
          |  }
          |}""".stripMargin)
    }

    it("should have none predicate filter for none predicates set") {
      val query = PartitionQuery("result", None, None)
      assert(query.predicateFilter.isEmpty)
    }

    it("should have always-false predicate filter for empty predicates set") {
      val query = PartitionQuery("result", Some(Set.empty), None)
      assert(query.predicateFilter.isDefined)
      assert(query.predicateFilter.get === "@filter(eq(true, false)) ")
    }

    it("should have predicate filter for given predicates set") {
      val query = PartitionQuery("result", Some(predicates), None)
      assert(query.predicateFilter.isDefined)
      assert(query.predicateFilter.get === "@filter(has(<prop1>) OR has(<prop2>) OR has(<edge1>) OR has(<edge2>)) ")
    }

    it("should have none predicate paths for none predicates set") {
      val query = PartitionQuery("result", None, None)
      assert(query.predicatePaths.isEmpty)
    }

    it("should have empty predicate paths for empty predicates set") {
      val query = PartitionQuery("result", Some(Set.empty), None)
      assert(query.predicatePaths.isDefined)
      assert(query.predicatePaths.get === "")
    }

    it("should have predicate paths for given predicates set") {
      val query = PartitionQuery("result", Some(predicates), None)
      assert(query.predicatePaths.isDefined)
      assert(query.predicatePaths.get === """    <prop1>
                                            |    <prop2>
                                            |    <edge1> { uid }
                                            |    <edge2> { uid }
                                            |""".stripMargin)
    }

  }
}
