/*
 * Copyright 2020 G-Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.DgraphTestCluster

import scala.jdk.CollectionConverters._

class TestClusterStateProvider extends AnyFunSpec with DgraphTestCluster {

  val options = new CaseInsensitiveStringMap(Map(IncludeReservedPredicatesOption -> "dgraph.type").asJava)

  describe("ClusterStateProvider") {

    it("should retrieve cluster state") {
      val provider = new ClusterStateProvider {}
      val state = provider.getClusterState(Target(dgraph.target))
      assert(state.isDefined === true)
      assert(state.get.groupMembers === Map("1" -> Set(Target(dgraph.target))))
      assert(state.get.groupPredicates.contains("1"))
      val actualPredicates = state.get.groupPredicates("1")
      val expectedPredicates = Set("name", "title", "starring", "running_time", "release_date", "director", "revenue", "dgraph.type")
      assert(expectedPredicates.diff(actualPredicates) === Set.empty)
      assert(state.get.maxUid.map(_.intValue()) === Some(10000))
    }

    it("should retrieve cluster states") {
      val provider = new ClusterStateProvider {}
      val state = provider.getClusterState(Seq(Target(dgraph.target), Target(dgraph.targetLocalIp)), options)
      assert(state.groupMembers === Map("1" -> Set(Target(dgraph.target))))
      assert(state.groupPredicates.contains("1"))
      val actualPredicates = state.groupPredicates("1")
      val expectedPredicates = Set("name", "title", "starring", "running_time", "release_date", "director", "revenue", "dgraph.type")
      assert(actualPredicates === expectedPredicates)
      assert(state.maxUid.map(_.intValue()) === Some(10000))
    }

    it("should fail for unavailable cluster") {
      val provider = new ClusterStateProvider {}
      assertThrows[RuntimeException] {
        provider.getClusterState(Seq(Target("localhost:1001")), options)
      }
    }

    it("should return None for unavailable cluster") {
      val provider = new ClusterStateProvider {}
      val state = provider.getClusterState(Target("localhost:1001"))
      assert(state.isEmpty)
    }

  }

}
