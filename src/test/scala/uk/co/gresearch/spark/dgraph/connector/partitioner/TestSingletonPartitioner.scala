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

package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.connector.{Partition, Predicate, Schema, Target}

class TestSingletonPartitioner extends AnyFunSpec {

  describe("SingletonPartitioner") {

    val targets = Seq(Target("host1:9080"), Target("host2:9080"))

    it("should partition") {
      val predicates = Set(Predicate("pred", "type", "type"))
      val schema = Schema(predicates)
      val partitioner = SingletonPartitioner(targets, schema)
      val partitions = partitioner.getPartitions

      assert(partitions.length === 1)
      assert(partitions.toSet === Set(Partition(targets).has(predicates)))
    }

    it("should provide lang directives") {
      val predicates = Set(Predicate("pred1", "type1", "type1"), Predicate("pred2", "type2", "type2", isLang = true))
      val schema = Schema(predicates)
      val partitioner = SingletonPartitioner(targets, schema)
      val partitions = partitioner.getPartitions

      assert(partitions.length === 1)
      assert(partitions.toSet === Set(Partition(targets).has(predicates).langs(Set("pred2"))))
    }

  }

}
