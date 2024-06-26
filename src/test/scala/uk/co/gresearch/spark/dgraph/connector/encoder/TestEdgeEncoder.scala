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

package uk.co.gresearch.spark.dgraph.connector.encoder

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.connector.{Json, Predicate, Schema, Uid}

class TestEdgeEncoder extends AnyFunSpec {

  describe("EdgeEncoder") {

    it("should encode edges") {
      val encoder = EdgeEncoder(Map.empty)
      val rowOpt = encoder.asInternalRow(Uid(1), "predicate", Uid(2))

      assert(rowOpt.isDefined === true)
      val row = rowOpt.get

      assert(row.numFields === 3)
      assert(row.getLong(0) === 1)
      assert(row.getUTF8String(1).toString === "predicate")
      assert(row.getLong(2) === 2)
    }

    it("should ignore node properties") {
      val encoder = EdgeEncoder(Map.empty)
      val rowOpt = encoder.asInternalRow(Uid(1), "predicate", 2L)
      assert(rowOpt.isEmpty)
    }

    it("should parse JSON response") {
      val encoder = EdgeEncoder(schema.predicateMap)
      val rows = encoder.fromJson(Json(json), "result")
      assert(
        rows.toSeq === Seq(
          InternalRow(1L, UTF8String.fromString("starring"), 2L),
          InternalRow(1L, UTF8String.fromString("starring"), 3L),
          InternalRow(1L, UTF8String.fromString("starring"), 7L),
          InternalRow(1L, UTF8String.fromString("director"), 4L),
          InternalRow(8L, UTF8String.fromString("starring"), 2L),
          InternalRow(8L, UTF8String.fromString("starring"), 3L),
          InternalRow(8L, UTF8String.fromString("starring"), 7L),
          InternalRow(8L, UTF8String.fromString("director"), 5L),
          InternalRow(9L, UTF8String.fromString("starring"), 2L),
          InternalRow(9L, UTF8String.fromString("starring"), 3L),
          InternalRow(9L, UTF8String.fromString("starring"), 7L),
          InternalRow(9L, UTF8String.fromString("director"), 6L),
        )
      )
    }

    it("should parse JSON response with large uids") {
      val encoder = EdgeEncoder(schema.predicateMap)
      val rows = encoder.fromJson(Json(jsonWithLargeUids), "result")
      assert(
        rows.toSeq === Seq(
          InternalRow(-6346846686373277921L, UTF8String.fromString("starring"), 3100520392254949455L),
          InternalRow(-6346846686373277921L, UTF8String.fromString("starring"), 999257064750498476L),
          InternalRow(-6346846686373277921L, UTF8String.fromString("starring"), -1877623327044447073L),
          InternalRow(-6346846686373277921L, UTF8String.fromString("director"), 8214320560726473464L),
        )
      )
    }

  }

}
