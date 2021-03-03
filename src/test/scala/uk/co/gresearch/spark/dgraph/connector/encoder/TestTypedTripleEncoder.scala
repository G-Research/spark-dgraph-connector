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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.connector.Predicate.sparkDataType
import uk.co.gresearch.spark.dgraph.connector._

class TestTypedTripleEncoder extends AnyFunSpec {

  Seq(
    (Uid(1), "1", "uid", "edges"),
    ("value", "value", "string", "string properties"),
    (123L, "123", "int", "long properties"),
    (123.456, "123.456", "float", "double properties"),
    (Timestamp.valueOf("2020-01-02 12:34:56.789"), "2020-01-02 12:34:56.789", "datetime", "dateTime properties"),
    (true, "true", "bool", "boolean properties"),
    (Geo("geo"), "geo", "geo", "geo properties"),
    (Password("secret"), "secret", "password", "password properties"),
    (new Object() {
      override def toString: String = "object"
    }, "object", "default", "default"),
  ).foreach { case (value, encoded, encType, test) =>

    it(s"should encode $test") {
      val schema = Schema(Set(Predicate("predicate", encType)))
      val encoder = TypedTripleEncoder(schema.predicateMap)
      val rowOpt = encoder.asInternalRow(Uid(1), "predicate", value)

      assert(rowOpt.isDefined === true)
      val row = rowOpt.get

      assert(row.numFields === 11)
      assert(row.getLong(0) === 1)
      assert(row.getString(1) === "predicate")
      if (encType == "uid") assert(row.getLong(2) === value.asInstanceOf[Uid].uid) else assert(row.get(2, StringType) === null)
      if (encType == "string" || encType == "default") assert(row.getUTF8String(3).toString === value.toString) else assert(row.get(3, StringType) === null)
      if (encType == "int") assert(row.getLong(4) === value) else assert(row.get(4, StringType) === null)
      if (encType == "float") assert(row.getDouble(5) === value) else assert(row.get(5, StringType) === null)
      if (encType == "datetime") assert(row.get(6, TimestampType) === DateTimeUtils.fromJavaTimestamp(value.asInstanceOf[Timestamp])) else assert(row.get(6, StringType) === null)
      if (encType == "bool") assert(row.getBoolean(7) === value) else assert(row.get(7, StringType) === null)
      if (encType == "geo") assert(row.getUTF8String(8).toString === value.asInstanceOf[Geo].geo) else assert(row.get(8, StringType) === null)
      if (encType == "password") assert(row.getUTF8String(9).toString === value.asInstanceOf[Password].password) else assert(row.get(9, StringType) === null)
      assert(row.getString(10) === sparkDataType(encType))
    }

  }

  it("should provide the expected read schema") {
    val encoder = TypedTripleEncoder(Map.empty)
    val expected = StructType(Seq(
      StructField("subject", LongType, nullable = false),
      StructField("predicate", StringType, nullable = false),
      StructField("objectUid", LongType),
      StructField("objectString", StringType),
      StructField("objectLong", LongType),
      StructField("objectDouble", DoubleType),
      StructField("objectTimestamp", TimestampType),
      StructField("objectBoolean", BooleanType),
      StructField("objectGeo", StringType),
      StructField("objectPassword", StringType),
      StructField("objectType", StringType, nullable = false)
    ))
    assert(encoder.readSchema() === expected)
  }

  it("should provide the expected schema") {
    val encoder = TypedTripleEncoder(Map.empty)
    val expected = StructType(Seq(
      StructField("subject", LongType, nullable = false),
      StructField("predicate", StringType, nullable = false),
      StructField("objectUid", LongType),
      StructField("objectString", StringType),
      StructField("objectLong", LongType),
      StructField("objectDouble", DoubleType),
      StructField("objectTimestamp", TimestampType),
      StructField("objectBoolean", BooleanType),
      StructField("objectGeo", StringType),
      StructField("objectPassword", StringType),
      StructField("objectType", StringType, nullable = false)
    ))
    assert(encoder.schema() === expected)
  }

  it("should parse JSON response") {
    val schema = Schema(Set(
      Predicate("name", "string"),
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

    def ts(string: String): Long = DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(string))

    val encoder = TypedTripleEncoder(schema.predicateMap)
    val rows = encoder.fromJson(Json(json), "result")
    assert(rows.toSeq === Seq(
      InternalRow(1L, UTF8String.fromString("name"), null, UTF8String.fromString("Star Wars: Episode IV - A New Hope"), null, null, null, null, null, null, UTF8String.fromString("string")),
      InternalRow(1L, UTF8String.fromString("release_date"), null, null, null, null, ts("1977-05-25 00:00:00"), null, null, null, UTF8String.fromString("timestamp")),
      InternalRow(1L, UTF8String.fromString("revenue"), null, null, null, 775000000.0, null, null, null, null, UTF8String.fromString("double")),
      InternalRow(1L, UTF8String.fromString("running_time"), null, null, 121L, null, null, null, null, null, UTF8String.fromString("long")),
      InternalRow(1L, UTF8String.fromString("starring"), 2L, null, null, null, null, null, null, null, UTF8String.fromString("uid")),
      InternalRow(1L, UTF8String.fromString("starring"), 3L, null, null, null, null, null, null, null, UTF8String.fromString("uid")),
      InternalRow(1L, UTF8String.fromString("starring"), 7L, null, null, null, null, null, null, null, UTF8String.fromString("uid")),
      InternalRow(1L, UTF8String.fromString("director"), 4L, null, null, null, null, null, null, null, UTF8String.fromString("uid")),
      InternalRow(2L, UTF8String.fromString("name"), null, UTF8String.fromString("Luke Skywalker"), null, null, null, null, null, null, UTF8String.fromString("string")),
      InternalRow(3L, UTF8String.fromString("name"), null, UTF8String.fromString("Han Solo"), null, null, null, null, null, null, UTF8String.fromString("string")),
      InternalRow(4L, UTF8String.fromString("name"), null, UTF8String.fromString("George Lucas"), null, null, null, null, null, null, UTF8String.fromString("string")),
      InternalRow(5L, UTF8String.fromString("name"), null, UTF8String.fromString("Irvin Kernshner"), null, null, null, null, null, null, UTF8String.fromString("string")),
      InternalRow(6L, UTF8String.fromString("name"), null, UTF8String.fromString("Richard Marquand"), null, null, null, null, null, null, UTF8String.fromString("string")),
      InternalRow(7L, UTF8String.fromString("name"), null, UTF8String.fromString("Princess Leia"), null, null, null, null, null, null, UTF8String.fromString("string")),
      InternalRow(8L, UTF8String.fromString("name"), null, UTF8String.fromString("Star Wars: Episode V - The Empire Strikes Back"), null, null, null, null, null, null, UTF8String.fromString("string")),
      InternalRow(8L, UTF8String.fromString("release_date"), null, null, null, null, ts("1980-05-21 00:00:00"), null, null, null, UTF8String.fromString("timestamp")),
      InternalRow(8L, UTF8String.fromString("revenue"), null, null, null, 534000000.0, null, null, null, null, UTF8String.fromString("double")),
      InternalRow(8L, UTF8String.fromString("running_time"), null, null, 124L, null, null, null, null, null, UTF8String.fromString("long")),
      InternalRow(8L, UTF8String.fromString("starring"), 2L, null, null, null, null, null, null, null, UTF8String.fromString("uid")),
      InternalRow(8L, UTF8String.fromString("starring"), 3L, null, null, null, null, null, null, null, UTF8String.fromString("uid")),
      InternalRow(8L, UTF8String.fromString("starring"), 7L, null, null, null, null, null, null, null, UTF8String.fromString("uid")),
      InternalRow(8L, UTF8String.fromString("director"), 5L, null, null, null, null, null, null, null, UTF8String.fromString("uid")),
      InternalRow(9L, UTF8String.fromString("name"), null, UTF8String.fromString("Star Wars: Episode VI - Return of the Jedi"), null, null, null, null, null, null, UTF8String.fromString("string")),
      InternalRow(9L, UTF8String.fromString("release_date"), null, null, null, null, ts("1983-05-25 00:00:00"), null, null, null, UTF8String.fromString("timestamp")),
      InternalRow(9L, UTF8String.fromString("revenue"), null, null, null, 572000000.0, null, null, null, null, UTF8String.fromString("double")),
      InternalRow(9L, UTF8String.fromString("running_time"), null, null, 131L, null, null, null, null, null, UTF8String.fromString("long")),
      InternalRow(9L, UTF8String.fromString("starring"), 2L, null, null, null, null, null, null, null, UTF8String.fromString("uid")),
      InternalRow(9L, UTF8String.fromString("starring"), 3L, null, null, null, null, null, null, null, UTF8String.fromString("uid")),
      InternalRow(9L, UTF8String.fromString("starring"), 7L, null, null, null, null, null, null, null, UTF8String.fromString("uid")),
      InternalRow(9L, UTF8String.fromString("director"), 6L, null, null, null, null, null, null, null, UTF8String.fromString("uid")),
      InternalRow(10L, UTF8String.fromString("name"), null, UTF8String.fromString("Star Trek: The Motion Picture"), null, null, null, null, null, null, UTF8String.fromString("string")),
      InternalRow(10L, UTF8String.fromString("release_date"), null, null, null, null, ts("1979-12-07 00:00:00"), null, null, null, UTF8String.fromString("timestamp")),
      InternalRow(10L, UTF8String.fromString("revenue"), null, null, null, 139000000.0, null, null, null, null, UTF8String.fromString("double")),
      InternalRow(10L, UTF8String.fromString("running_time"), null, null, 132L, null, null, null, null, null, UTF8String.fromString("long")),
    ))
  }

}
