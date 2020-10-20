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
      val schema = Schema(Set(
        Predicate("release_date", "datetime"),
        Predicate("revenue", "float"),
        Predicate("running_time", "int"),
        Predicate("director", "uid"),
        Predicate("starring", "uid")
      ))

      val json =
        """
          |{
          |    "result": [
          |      {
          |        "uid": "0x1",
          |        "name": "Star Wars: Episode IV - A New Hope",
          |        "release_date": "1977-05-25T00:00:00Z",
          |        "revenue": "775000000",
          |        "running_time": 121,
          |        "starring": [
          |          {
          |            "uid": "0x2"
          |          },
          |          {
          |            "uid": "0x3"
          |          },
          |          {
          |            "uid": "0x7"
          |          }
          |        ],
          |        "director": [
          |          {
          |            "uid": "0x4"
          |          }
          |        ]
          |      },
          |      {
          |        "uid": "0x2",
          |        "name": "Luke Skywalker"
          |      },
          |      {
          |        "uid": "0x3",
          |        "name": "Han Solo"
          |      },
          |      {
          |        "uid": "0x4",
          |        "name": "George Lucas"
          |      },
          |      {
          |        "uid": "0x5",
          |        "name": "Irvin Kernshner"
          |      },
          |      {
          |        "uid": "0x6",
          |        "name": "Richard Marquand"
          |      },
          |      {
          |        "uid": "0x7",
          |        "name": "Princess Leia"
          |      },
          |      {
          |        "uid": "0x8",
          |        "name": "Star Wars: Episode V - The Empire Strikes Back",
          |        "release_date": "1980-05-21T00:00:00Z",
          |        "revenue": "534000000",
          |        "running_time": 124,
          |        "starring": [
          |          {
          |            "uid": "0x2"
          |          },
          |          {
          |            "uid": "0x3"
          |          },
          |          {
          |            "uid": "0x7"
          |          }
          |        ],
          |        "director": [
          |          {
          |            "uid": "0x5"
          |          }
          |        ]
          |      },
          |      {
          |        "uid": "0x9",
          |        "name": "Star Wars: Episode VI - Return of the Jedi",
          |        "release_date": "1983-05-25T00:00:00Z",
          |        "revenue": "572000000",
          |        "running_time": 131,
          |        "starring": [
          |          {
          |            "uid": "0x2"
          |          },
          |          {
          |            "uid": "0x3"
          |          },
          |          {
          |            "uid": "0x7"
          |          }
          |        ],
          |        "director": [
          |          {
          |            "uid": "0x6"
          |          }
          |        ]
          |      },
          |      {
          |        "uid": "0xa",
          |        "name": "Star Trek: The Motion Picture",
          |        "release_date": "1979-12-07T00:00:00Z",
          |        "revenue": "139000000",
          |        "running_time": 132
          |      }
          |    ]
          |  }""".stripMargin

      val encoder = EdgeEncoder(schema.predicateMap)
      val rows = encoder.fromJson(Json(json), "result")
      assert(rows.toSeq === Seq(
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
      ))
    }

  }

}
