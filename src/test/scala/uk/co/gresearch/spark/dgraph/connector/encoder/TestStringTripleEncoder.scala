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
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.connector.Predicate.sparkDataType
import uk.co.gresearch.spark.dgraph.connector._

class TestStringTripleEncoder extends AnyFunSpec {

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
      val encoder = StringTripleEncoder(schema.predicateMap)
      val row = encoder.asInternalRow(Uid(1), "predicate", value)

      assert(row.isDefined === true)
      assert(row.get.numFields === 4)
      assert(row.get.getLong(0) === 1)
      assert(row.get.getString(1) === "predicate")
      assert(row.get.getString(2) === encoded)
      assert(row.get.getString(3) === sparkDataType(encType))
    }

  }

  it("should provide the expected read schema") {
    val encoder = StringTripleEncoder(Map.empty)
    val expected = StructType(Seq(
      StructField("subject", LongType, nullable = false),
      StructField("predicate", StringType, nullable = false),
      StructField("objectString", StringType, nullable = false),
      StructField("objectType", StringType, nullable = false)
    ))
    assert(encoder.readSchema() === expected)
  }

  it("should provide the expected schema") {
    val encoder = StringTripleEncoder(Map.empty)
    val expected = StructType(Seq(
      StructField("subject", LongType, nullable = false),
      StructField("predicate", StringType, nullable = false),
      StructField("objectString", StringType, nullable = false),
      StructField("objectType", StringType, nullable = false)
    ))
    assert(encoder.schema() === expected)
  }

  it("should parse JSON response") {
    val encoder = StringTripleEncoder(schema.predicateMap)
    val rows = encoder.fromJson(Json(json), "result")
    assert(rows.toSeq === Seq(
      InternalRow(1L, UTF8String.fromString("name"), UTF8String.fromString("Star Wars: Episode IV - A New Hope"), UTF8String.fromString("string")),
      InternalRow(1L, UTF8String.fromString("release_date"), UTF8String.fromString("1977-05-25 00:00:00.0"), UTF8String.fromString("timestamp")),
      InternalRow(1L, UTF8String.fromString("revenue"), UTF8String.fromString("7.75E8"), UTF8String.fromString("double")),
      InternalRow(1L, UTF8String.fromString("running_time"), UTF8String.fromString("121"), UTF8String.fromString("long")),
      InternalRow(1L, UTF8String.fromString("starring"), UTF8String.fromString("2"), UTF8String.fromString("uid")),
      InternalRow(1L, UTF8String.fromString("starring"), UTF8String.fromString("3"), UTF8String.fromString("uid")),
      InternalRow(1L, UTF8String.fromString("starring"), UTF8String.fromString("7"), UTF8String.fromString("uid")),
      InternalRow(1L, UTF8String.fromString("director"), UTF8String.fromString("4"), UTF8String.fromString("uid")),
      InternalRow(2L, UTF8String.fromString("name"), UTF8String.fromString("Luke Skywalker"), UTF8String.fromString("string")),
      InternalRow(3L, UTF8String.fromString("name"), UTF8String.fromString("Han Solo"), UTF8String.fromString("string")),
      InternalRow(4L, UTF8String.fromString("name"), UTF8String.fromString("George Lucas"), UTF8String.fromString("string")),
      InternalRow(5L, UTF8String.fromString("name"), UTF8String.fromString("Irvin Kernshner"), UTF8String.fromString("string")),
      InternalRow(6L, UTF8String.fromString("name"), UTF8String.fromString("Richard Marquand"), UTF8String.fromString("string")),
      InternalRow(7L, UTF8String.fromString("name"), UTF8String.fromString("Princess Leia"), UTF8String.fromString("string")),
      InternalRow(8L, UTF8String.fromString("name"), UTF8String.fromString("Star Wars: Episode V - The Empire Strikes Back"), UTF8String.fromString("string")),
      InternalRow(8L, UTF8String.fromString("release_date"), UTF8String.fromString("1980-05-21 00:00:00.0"), UTF8String.fromString("timestamp")),
      InternalRow(8L, UTF8String.fromString("revenue"), UTF8String.fromString("5.34E8"), UTF8String.fromString("double")),
      InternalRow(8L, UTF8String.fromString("running_time"), UTF8String.fromString("124"), UTF8String.fromString("long")),
      InternalRow(8L, UTF8String.fromString("starring"), UTF8String.fromString("2"), UTF8String.fromString("uid")),
      InternalRow(8L, UTF8String.fromString("starring"), UTF8String.fromString("3"), UTF8String.fromString("uid")),
      InternalRow(8L, UTF8String.fromString("starring"), UTF8String.fromString("7"), UTF8String.fromString("uid")),
      InternalRow(8L, UTF8String.fromString("director"), UTF8String.fromString("5"), UTF8String.fromString("uid")),
      InternalRow(9L, UTF8String.fromString("name"), UTF8String.fromString("Star Wars: Episode VI - Return of the Jedi"), UTF8String.fromString("string")),
      InternalRow(9L, UTF8String.fromString("release_date"), UTF8String.fromString("1983-05-25 00:00:00.0"), UTF8String.fromString("timestamp")),
      InternalRow(9L, UTF8String.fromString("revenue"), UTF8String.fromString("5.72E8"), UTF8String.fromString("double")),
      InternalRow(9L, UTF8String.fromString("running_time"), UTF8String.fromString("131"), UTF8String.fromString("long")),
      InternalRow(9L, UTF8String.fromString("starring"), UTF8String.fromString("2"), UTF8String.fromString("uid")),
      InternalRow(9L, UTF8String.fromString("starring"), UTF8String.fromString("3"), UTF8String.fromString("uid")),
      InternalRow(9L, UTF8String.fromString("starring"), UTF8String.fromString("7"), UTF8String.fromString("uid")),
      InternalRow(9L, UTF8String.fromString("director"), UTF8String.fromString("6"), UTF8String.fromString("uid")),
      InternalRow(10L, UTF8String.fromString("name"), UTF8String.fromString("Star Trek: The Motion Picture"), UTF8String.fromString("string")),
      InternalRow(10L, UTF8String.fromString("release_date"), UTF8String.fromString("1979-12-07 00:00:00.0"), UTF8String.fromString("timestamp")),
      InternalRow(10L, UTF8String.fromString("revenue"), UTF8String.fromString("1.39E8"), UTF8String.fromString("double")),
      InternalRow(10L, UTF8String.fromString("running_time"), UTF8String.fromString("132"), UTF8String.fromString("long")),
    ))
  }

  it("should parse JSON response with large uids") {
    val encoder = StringTripleEncoder(schema.predicateMap)
    val rows = encoder.fromJson(Json(jsonWithLargeUids), "result")
    assert(rows.toSeq === Seq(
      InternalRow(-6346846686373277921L, UTF8String.fromString("name"), UTF8String.fromString("Star Wars: Episode IV - A New Hope"), UTF8String.fromString("string")),
      InternalRow(-6346846686373277921L, UTF8String.fromString("release_date"), UTF8String.fromString("1977-05-25 00:00:00.0"), UTF8String.fromString("timestamp")),
      InternalRow(-6346846686373277921L, UTF8String.fromString("revenue"), UTF8String.fromString("7.75E8"), UTF8String.fromString("double")),
      InternalRow(-6346846686373277921L, UTF8String.fromString("running_time"), UTF8String.fromString("121"), UTF8String.fromString("long")),
      InternalRow(-6346846686373277921L, UTF8String.fromString("starring"), UTF8String.fromString("3100520392254949455"), UTF8String.fromString("uid")),
      InternalRow(-6346846686373277921L, UTF8String.fromString("starring"), UTF8String.fromString("999257064750498476"), UTF8String.fromString("uid")),
      InternalRow(-6346846686373277921L, UTF8String.fromString("starring"), UTF8String.fromString("-1877623327044447073"), UTF8String.fromString("uid")),
      InternalRow(-6346846686373277921L, UTF8String.fromString("director"), UTF8String.fromString("8214320560726473464"), UTF8String.fromString("uid")),
    ))
  }

}
