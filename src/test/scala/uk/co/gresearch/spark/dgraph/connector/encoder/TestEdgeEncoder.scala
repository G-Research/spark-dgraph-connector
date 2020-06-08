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

package uk.co.gresearch.spark.dgraph.connector.encoder

import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.{Triple, Uid}

class TestEdgeEncoder extends FunSpec {

  describe("EdgeEncoder") {

    it("should encode edges") {
      val encoder = EdgeEncoder()
      val edge = Triple(Uid(1), "predicate", Uid(2))
      val row = encoder.asInternalRow(edge)

      assert(row.numFields === 3)
      assert(row.getLong(0) === 1)
      assert(row.getUTF8String(1).toString === "predicate")
      assert(row.getLong(2) === 2)
    }

    it("should fail on node properties") {
      val encoder = EdgeEncoder()
      val edge = Triple(Uid(1), "predicate", 2L)
      assertThrows[IllegalArgumentException]{ encoder.asInternalRow(edge) }
    }

  }
}
