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

class TestPartition extends FunSpec with SchemaProvider {

  describe("Partition") {

    Seq(1, 3, 10, 30, 100, 300, 1000, 3000, 10000).foreach { predicates =>

      // test that a partition works with N predicates
      // the query grows linearly with N, so does the processing time
      it(s"should read $predicates predicates") {
        val targets = Seq(Target("localhost:9080"))
        val existingPredicates = getSchema(targets).predicates.slice(0, predicates)
        val syntheticPredicates =
          (1 to (predicates - existingPredicates.size)).map(pred =>
            Predicate(s"predicate$pred", if (pred % 2 == 0) "string" else "uid")
          ).toSet
        val schema = Schema(syntheticPredicates ++ existingPredicates)
        val partition = Partition(targets, Option(schema.predicates), None)
        val triplesFactory = TriplesFactory(schema)
        println(partition.getTriples(triplesFactory).length)
      }

    }

  }

}
