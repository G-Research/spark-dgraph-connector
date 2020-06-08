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

import java.sql.Timestamp

import org.apache.spark.sql.types._
import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.{Geo, Password, Triple, Uid}

class TestStringTripleTripleEncoder extends FunSpec {

  Seq(
    (Uid(1), "1", "uid", "edges"),
    ("value", "value", "string", "string properties"),
    (123L, "123", "long", "long properties"),
    (123.456, "123.456", "double", "double properties"),
    (Timestamp.valueOf("2020-01-02 12:34:56.789"), "2020-01-02 12:34:56.789", "timestamp", "dateTime properties"),
    (true, "true", "boolean", "boolean properties"),
    (Geo("geo"), "geo", "geo", "geo properties"),
    (Password("secret"), "secret", "password", "password properties"),
    (new Object() {
      override def toString: String = "object"
    }, "object", "default", "default"),
  ).foreach { case (value, encoded, encType, test) =>

    it(s"should encode $test to internalrow") {
      val encoder = StringTripleEncoder()
      val triple = Triple(Uid(1), "predicate", value)
      val row = encoder.asInternalRow(triple)

      assert(row.numFields === 4)
      assert(row.getLong(0) === 1)
      assert(row.getString(1) === "predicate")
      assert(row.getString(2) === encoded)
      assert(row.getString(3) === encType)
    }

  }

  it("should provide the expected read schema") {
    val encoder = StringTripleEncoder()
    val expected = StructType(Seq(
      StructField("subject", LongType, nullable = false),
      StructField("predicate", StringType),
      StructField("objectString", StringType),
      StructField("objectType", StringType)
    ))
    assert(encoder.readSchema() === expected)
  }

  it("should provide the expected schema") {
    val encoder = StringTripleEncoder()
    val expected = StructType(Seq(
      StructField("subject", LongType, nullable = false),
      StructField("predicate", StringType),
      StructField("objectString", StringType),
      StructField("objectType", StringType)
    ))
    assert(encoder.schema() === expected)
  }

}
