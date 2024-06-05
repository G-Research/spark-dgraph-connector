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

package uk.co.gresearch.spark.dgraph.connector

import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.connector.ReservedPredicateFilter.getPredicateFilters

import java.util.regex.Pattern

class TestReservedPredicateFilter extends AnyFunSpec {

  describe("SchemaProvider") {

    it("should provide predicate filters") {
      assert(getPredicateFilters("dgraph.*").map(_.pattern) === Set("dgraph\\..*"))
      assert(getPredicateFilters("dgraph.predicate").map(_.pattern) === Set("dgraph\\.predicate"))
      assert(
        getPredicateFilters("dgraph.predicate1,dgraph.predicate2").map(_.pattern) === Set(
          "dgraph\\.predicate1",
          "dgraph\\.predicate2"
        )
      )
      assert(getPredicateFilters("dgraph.predicate.name").map(_.pattern) === Set("dgraph\\.predicate\\.name"))
      assert(getPredicateFilters("dgraph.predicate.*").map(_.pattern) === Set("dgraph\\.predicate\\..*"))
      assert(
        getPredicateFilters("dgraph.predicate.*,dgraph.predicate.name").map(_.pattern) === Set(
          "dgraph\\.predicate\\..*",
          "dgraph\\.predicate\\.name"
        )
      )
      assert(getPredicateFilters("dgraph.predicate[abc]?").map(_.pattern) === Set("dgraph\\.predicate\\[abc\\]\\?"))
      assert(
        getPredicateFilters("dgraph.predicate[abc]{2}").map(_.pattern) === Set("dgraph\\.predicate\\[abc\\]\\{2\\}")
      )
    }

    it("should deny non-reserved predicate filters") {
      assertThrows[IllegalArgumentException] { getPredicateFilters("*") }
      assertThrows[IllegalArgumentException] { getPredicateFilters("predicate") }
      assertThrows[IllegalArgumentException] { getPredicateFilters("dgraph*") }
      assertThrows[IllegalArgumentException] { getPredicateFilters("dgraph.predicate1,predicate2") }
    }

  }

}
