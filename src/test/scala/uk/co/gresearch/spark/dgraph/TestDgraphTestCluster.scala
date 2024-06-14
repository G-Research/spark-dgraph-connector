/*
 * Copyright 2021 G-Research
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

package uk.co.gresearch.spark.dgraph

import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.connector.{
  DgraphDataFrameReader,
  TriplesModeOption,
  TriplesModeTypedOption,
  TypedTriple
}

class TestDgraphTestCluster extends AnyFunSpec with SparkTestSession with DgraphTestCluster {

  describe("DgraphTestCluster") {

    import spark.implicits._

    it("should have sufficient reserved predicates for testing filtering reserved predicates") {
      val availablePredicates =
        spark.read
          .option(TriplesModeOption, TriplesModeTypedOption)
          .dgraph
          .triples(dgraph.target)
          .as[TypedTriple]
          .collect()
          .map(_.predicate)
          .toSet

      // TestEdgeSource, TestNodeSource and TestTripleSource assume these predicates exist
      assert(availablePredicates.contains("dgraph.type"))
      assert(availablePredicates.contains("dgraph.graphql.schema"))
      assert(availablePredicates.contains("dgraph.graphql.xid"))
    }

  }

}
