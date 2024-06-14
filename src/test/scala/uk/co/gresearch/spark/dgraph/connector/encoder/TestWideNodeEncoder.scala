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

import java.sql.Timestamp

import com.google.gson.JsonObject
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.connector.{Geo, Json, Password, Predicate, Schema, Uid}

class TestWideNodeEncoder extends AnyFunSpec {

  describe("WideNodeEncoder") {

    val predicates = Set(
      Predicate("name", "string"),
      Predicate("release_date", "datetime"),
      Predicate("revenue", "float"),
      Predicate("running_time", "int")
    )

    val projectedPredicates = Set(
      Predicate("name", "string"),
      Predicate("release_date", "datetime")
    )

    val expectedProjectedReadPredicates = Seq(
      Predicate("uid", "subject")
    ) ++ projectedPredicates.toSeq

    val expectedSchema = StructType(
      Seq(
        StructField("subject", LongType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("release_date", TimestampType, nullable = true),
        StructField("revenue", DoubleType, nullable = true),
        StructField("running_time", LongType, nullable = true)
      )
    )

    val expectedProjectedSchema = StructType(
      Seq(
        StructField("subject", LongType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("release_date", TimestampType, nullable = true)
      )
    )

    val encoder = WideNodeEncoder(predicates)

    val fullJson =
      """{
        |    "result": [
        |      {
        |        "uid": "0x1",
        |        "name": "Star Wars: Episode IV - A New Hope",
        |        "release_date": "1977-05-25T00:00:00Z",
        |        "revenue": "775000000",
        |        "running_time": 121
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
        |        "running_time": 124
        |      },
        |      {
        |        "uid": "0x9",
        |        "name": "Star Wars: Episode VI - Return of the Jedi",
        |        "release_date": "1983-05-25T00:00:00Z",
        |        "revenue": "572000000",
        |        "running_time": 131
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

    // only name, release_date and director selected
    val projectedJsonWithEdges =
      """{
        |    "result": [
        |      {
        |        "uid": "0x1",
        |        "name": "Star Wars: Episode IV - A New Hope",
        |        "release_date": "1977-05-25T00:00:00Z",
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
        |        "director": [
        |          {
        |            "uid": "0x6"
        |          }
        |        ]
        |      },
        |      {
        |        "uid": "0xa",
        |        "name": "Star Trek: The Motion Picture",
        |        "release_date": "1979-12-07T00:00:00Z"
        |      }
        |    ]
        |  }""".stripMargin

    def ts(string: String): Long = DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(string))

    val expectedRowsFullSchema = Seq(
      InternalRow(
        1,
        UTF8String.fromString("Star Wars: Episode IV - A New Hope"),
        ts("1977-05-25 00:00:00"),
        7.75e8,
        121
      ),
      InternalRow(2, UTF8String.fromString("Luke Skywalker"), null, null, null),
      InternalRow(3, UTF8String.fromString("Han Solo"), null, null, null),
      InternalRow(4, UTF8String.fromString("George Lucas"), null, null, null),
      InternalRow(5, UTF8String.fromString("Irvin Kernshner"), null, null, null),
      InternalRow(6, UTF8String.fromString("Richard Marquand"), null, null, null),
      InternalRow(7, UTF8String.fromString("Princess Leia"), null, null, null),
      InternalRow(
        8,
        UTF8String.fromString("Star Wars: Episode V - The Empire Strikes Back"),
        ts("1980-05-21 00:00:00"),
        5.34e8,
        124
      ),
      InternalRow(
        9,
        UTF8String.fromString("Star Wars: Episode VI - Return of the Jedi"),
        ts("1983-05-25 00:00:00"),
        5.72e8,
        131
      ),
      InternalRow(10, UTF8String.fromString("Star Trek: The Motion Picture"), ts("1979-12-07 00:00:00"), 1.39e8, 132),
    )

    val expectedRowsFullSchemaProjectedJson = Seq(
      InternalRow(
        1,
        UTF8String.fromString("Star Wars: Episode IV - A New Hope"),
        ts("1977-05-25 00:00:00"),
        null,
        null
      ),
      InternalRow(2, UTF8String.fromString("Luke Skywalker"), null, null, null),
      InternalRow(3, UTF8String.fromString("Han Solo"), null, null, null),
      InternalRow(4, UTF8String.fromString("George Lucas"), null, null, null),
      InternalRow(5, UTF8String.fromString("Irvin Kernshner"), null, null, null),
      InternalRow(6, UTF8String.fromString("Richard Marquand"), null, null, null),
      InternalRow(7, UTF8String.fromString("Princess Leia"), null, null, null),
      InternalRow(
        8,
        UTF8String.fromString("Star Wars: Episode V - The Empire Strikes Back"),
        ts("1980-05-21 00:00:00"),
        null,
        null
      ),
      InternalRow(
        9,
        UTF8String.fromString("Star Wars: Episode VI - Return of the Jedi"),
        ts("1983-05-25 00:00:00"),
        null,
        null
      ),
      InternalRow(10, UTF8String.fromString("Star Trek: The Motion Picture"), ts("1979-12-07 00:00:00"), null, null),
    )

    val expectedRowsProjectedSchema = Seq(
      InternalRow(1, UTF8String.fromString("Star Wars: Episode IV - A New Hope"), ts("1977-05-25 00:00:00")),
      InternalRow(2, UTF8String.fromString("Luke Skywalker"), null),
      InternalRow(3, UTF8String.fromString("Han Solo"), null),
      InternalRow(4, UTF8String.fromString("George Lucas"), null),
      InternalRow(5, UTF8String.fromString("Irvin Kernshner"), null),
      InternalRow(6, UTF8String.fromString("Richard Marquand"), null),
      InternalRow(7, UTF8String.fromString("Princess Leia"), null),
      InternalRow(
        8,
        UTF8String.fromString("Star Wars: Episode V - The Empire Strikes Back"),
        ts("1980-05-21 00:00:00")
      ),
      InternalRow(9, UTF8String.fromString("Star Wars: Episode VI - Return of the Jedi"), ts("1983-05-25 00:00:00")),
      InternalRow(10, UTF8String.fromString("Star Trek: The Motion Picture"), ts("1979-12-07 00:00:00")),
    )

    it("should ignore edges") {
      val edgeEncoder = WideNodeEncoder(
        predicates ++ Set(
          Predicate("director", "uid"),
          Predicate("starring", "uid")
        )
      )

      assert(edgeEncoder.schema === encoder.schema)
      assert(edgeEncoder.readSchema === encoder.readSchema)
    }

    it("should parse JSON response") {
      val rows = encoder.fromJson(Json(fullJson), "result").toSeq
      assert(rows === expectedRowsFullSchema)
      assert(encoder.schema === expectedSchema)
      assert(encoder.readSchema === expectedSchema)
    }

    it("should parse JSON response and ignore edges") {
      val rows = encoder.fromJson(Json(json), "result").toSeq
      assert(rows === expectedRowsFullSchema)
      assert(encoder.schema === expectedSchema)
      assert(encoder.readSchema === expectedSchema)
    }

    it("should parse JSON response with large uids") {
      val rows = encoder.fromJson(Json(jsonWithLargeUids), "result").toSeq
      val expected = Seq(
        InternalRow(
          -6346846686373277921L,
          UTF8String.fromString("Star Wars: Episode IV - A New Hope"),
          ts("1977-05-25 00:00:00"),
          7.75e8,
          121
        ),
      )
      assert(rows === expected)
      assert(encoder.schema === expectedSchema)
      assert(encoder.readSchema === expectedSchema)
    }

    it("should parse projected JSON response with all predicates") {
      val rows = encoder.fromJson(Json(projectedJsonWithEdges), "result").toSeq
      assert(rows === expectedRowsFullSchemaProjectedJson)
      assert(encoder.schema === expectedSchema)
      assert(encoder.readSchema === expectedSchema)
    }

    it("should parse projected JSON response with projected predicates") {
      val projectedEncoder = WideNodeEncoder(projectedPredicates)
      val rows = projectedEncoder.fromJson(Json(projectedJsonWithEdges), "result").toSeq
      assert(rows === expectedRowsProjectedSchema)
      assert(projectedEncoder.schema === expectedProjectedSchema)
      assert(projectedEncoder.readSchema === expectedProjectedSchema)
      assert(projectedEncoder.readPredicates === None)
    }

    it("should parse JSON response with projected predicates") {
      val projectedEncoder = WideNodeEncoder(projectedPredicates)
      val rows = projectedEncoder.fromJson(Json(fullJson), "result").toSeq
      assert(rows === expectedRowsProjectedSchema)
      assert(projectedEncoder.schema === expectedProjectedSchema)
      assert(projectedEncoder.readSchema === expectedProjectedSchema)
      assert(projectedEncoder.readPredicates === None)
    }

    it("should parse JSON response with all predicates and projected schema") {
      val projectedEncoder = WideNodeEncoder(predicates, Some(expectedProjectedSchema))
      val rows = projectedEncoder.fromJson(Json(fullJson), "result").toSeq
      assert(rows === expectedRowsProjectedSchema)
      assert(projectedEncoder.schema === encoder.schema)
      assert(projectedEncoder.readSchema === expectedProjectedSchema)
      assert(projectedEncoder.readPredicates === Some(expectedProjectedReadPredicates))
    }

    it("should parse projected JSON response with all predicates and projected schema") {
      val projectedEncoder = WideNodeEncoder(predicates, Some(expectedProjectedSchema))
      val rows = projectedEncoder.fromJson(Json(projectedJsonWithEdges), "result").toSeq
      assert(rows === expectedRowsProjectedSchema)
      assert(projectedEncoder.schema === encoder.schema)
      assert(projectedEncoder.readSchema === expectedProjectedSchema)
      assert(projectedEncoder.readPredicates === Some(expectedProjectedReadPredicates))
    }

    it("should ignore projection of full schema") {
      val projectedEncoder = WideNodeEncoder(predicates, Some(encoder.schema))
      assert(projectedEncoder.readPredicates === None)
    }

  }

}
