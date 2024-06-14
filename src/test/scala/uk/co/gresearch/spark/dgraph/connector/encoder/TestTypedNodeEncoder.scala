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
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.connector.{Geo, Json, Password, Predicate, Schema, Uid}

class TestTypedNodeEncoder extends AnyFunSpec {

  describe("TypedNodeEncoder") {

    Seq(
      ("value", "value", "string", "string properties"),
      (123L, "123", "long", "long properties"),
      (123.456, "123.456", "double", "double properties"),
      (Timestamp.valueOf("2020-01-02 12:34:56.789"), "2020-01-02 12:34:56.789", "timestamp", "dateTime properties"),
      (true, "true", "boolean", "boolean properties"),
      (Geo("geo"), "geo", "geo", "geo properties"),
      (Password("secret"), "secret", "password", "password properties"),
      (
        new Object() {
          override def toString: String = "object"
        },
        "object",
        "default",
        "default"
      ),
    ).foreach { case (value, encoded, encType, test) =>
      it(s"should encode $test") {
        val encoder = TypedNodeEncoder(Map.empty)
        val rowOpt = encoder.asInternalRow(Uid(1), "predicate", value)

        assert(rowOpt.isDefined === true)
        val row = rowOpt.get

        assert(row.numFields === 10)
        assert(row.getLong(0) === 1)
        assert(row.getString(1) === "predicate")
        // format: off
        if (encType == "string" || encType == "default") assert(row.getUTF8String(2).toString === value.toString) else assert(row.get(2, StringType) === null)
        if (encType == "long") assert(row.getLong(3) === value) else assert(row.get(3, StringType) === null)
        if (encType == "double") assert(row.getDouble(4) === value) else assert(row.get(4, StringType) === null)
        if (encType == "timestamp") assert(row.get(5, TimestampType) === DateTimeUtils.fromJavaTimestamp(value.asInstanceOf[Timestamp])) else assert(row.get(5, StringType) === null)
        if (encType == "boolean") assert(row.getBoolean(6) === value) else assert(row.get(6, StringType) === null)
        if (encType == "geo") assert(row.getUTF8String(7).toString === value.asInstanceOf[Geo].geo) else assert(row.get(7, StringType) === null)
        if (encType == "password") assert(row.getUTF8String(8).toString === value.asInstanceOf[Password].password) else assert(row.get(8, StringType) === null)
        // format: on
        assert(row.getString(9) === encType)
      }

    }

    it("should ignore edges") {
      val encoder = TypedNodeEncoder(Map.empty)
      val rowOpt = encoder.asInternalRow(Uid(1), "predicate", Uid(2))
      assert(rowOpt.isEmpty)
    }

    it("should parse JSON response") {
      def ts(string: String): Long = DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(string))

      val encoder = TypedNodeEncoder(schema.predicateMap)
      val rows = encoder.fromJson(Json(json), "result")
      // format: off
      assert(rows.toSeq === Seq(
        InternalRow(1L, UTF8String.fromString("name"), UTF8String.fromString("Star Wars: Episode IV - A New Hope"), null, null, null, null, null, null, UTF8String.fromString("string")),
        InternalRow(1L, UTF8String.fromString("release_date"), null, null, null, ts("1977-05-25 00:00:00"), null, null, null, UTF8String.fromString("timestamp")),
        InternalRow(1L, UTF8String.fromString("revenue"), null, null, 775000000.0, null, null, null, null, UTF8String.fromString("double")),
        InternalRow(1L, UTF8String.fromString("running_time"), null, 121L, null, null, null, null, null, UTF8String.fromString("long")),
        InternalRow(2L, UTF8String.fromString("name"), UTF8String.fromString("Luke Skywalker"), null, null, null, null, null, null, UTF8String.fromString("string")),
        InternalRow(3L, UTF8String.fromString("name"), UTF8String.fromString("Han Solo"), null, null, null, null, null, null, UTF8String.fromString("string")),
        InternalRow(4L, UTF8String.fromString("name"), UTF8String.fromString("George Lucas"), null, null, null, null, null, null, UTF8String.fromString("string")),
        InternalRow(5L, UTF8String.fromString("name"), UTF8String.fromString("Irvin Kernshner"), null, null, null, null, null, null, UTF8String.fromString("string")),
        InternalRow(6L, UTF8String.fromString("name"), UTF8String.fromString("Richard Marquand"), null, null, null, null, null, null, UTF8String.fromString("string")),
        InternalRow(7L, UTF8String.fromString("name"), UTF8String.fromString("Princess Leia"), null, null, null, null, null, null, UTF8String.fromString("string")),
        InternalRow(8L, UTF8String.fromString("name"), UTF8String.fromString("Star Wars: Episode V - The Empire Strikes Back"), null, null, null, null, null, null, UTF8String.fromString("string")),
        InternalRow(8L, UTF8String.fromString("release_date"), null, null, null, ts("1980-05-21 00:00:00"), null, null, null, UTF8String.fromString("timestamp")),
        InternalRow(8L, UTF8String.fromString("revenue"), null, null, 534000000.0, null, null, null, null, UTF8String.fromString("double")),
        InternalRow(8L, UTF8String.fromString("running_time"), null, 124L, null, null, null, null, null, UTF8String.fromString("long")),
        InternalRow(9L, UTF8String.fromString("name"), UTF8String.fromString("Star Wars: Episode VI - Return of the Jedi"), null, null, null, null, null, null, UTF8String.fromString("string")),
        InternalRow(9L, UTF8String.fromString("release_date"), null, null, null, ts("1983-05-25 00:00:00"), null, null, null, UTF8String.fromString("timestamp")),
        InternalRow(9L, UTF8String.fromString("revenue"), null, null, 572000000.0, null, null, null, null, UTF8String.fromString("double")),
        InternalRow(9L, UTF8String.fromString("running_time"), null, 131L, null, null, null, null, null, UTF8String.fromString("long")),
        InternalRow(10L, UTF8String.fromString("name"), UTF8String.fromString("Star Trek: The Motion Picture"), null, null, null, null, null, null, UTF8String.fromString("string")),
        InternalRow(10L, UTF8String.fromString("release_date"), null, null, null, ts("1979-12-07 00:00:00"), null, null, null, UTF8String.fromString("timestamp")),
        InternalRow(10L, UTF8String.fromString("revenue"), null, null, 139000000.0, null, null, null, null, UTF8String.fromString("double")),
        InternalRow(10L, UTF8String.fromString("running_time"), null, 132L, null, null, null, null, null, UTF8String.fromString("long")),
      ))
      // format: on
    }

    it("should parse JSON response with large uids") {
      def ts(string: String): Long = DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(string))

      val encoder = TypedNodeEncoder(schema.predicateMap)
      val rows = encoder.fromJson(Json(jsonWithLargeUids), "result")
      // format: off
      assert(rows.toSeq === Seq(
        InternalRow(-6346846686373277921L, UTF8String.fromString("name"), UTF8String.fromString("Star Wars: Episode IV - A New Hope"), null, null, null, null, null, null, UTF8String.fromString("string")),
        InternalRow(-6346846686373277921L, UTF8String.fromString("release_date"), null, null, null, ts("1977-05-25 00:00:00"), null, null, null, UTF8String.fromString("timestamp")),
        InternalRow(-6346846686373277921L, UTF8String.fromString("revenue"), null, null, 775000000.0, null, null, null, null, UTF8String.fromString("double")),
        InternalRow(-6346846686373277921L, UTF8String.fromString("running_time"), null, 121L, null, null, null, null, null, UTF8String.fromString("long")),
      ))
      // format: on
    }
  }

}
