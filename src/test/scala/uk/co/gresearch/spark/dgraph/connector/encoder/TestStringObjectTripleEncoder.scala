package uk.co.gresearch.spark.dgraph.connector.encoder

import java.sql.Timestamp

import org.apache.spark.sql.types._
import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.{Geo, Password, Triple, Uid}

class TestStringObjectTripleEncoder extends FunSpec {

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
      val encoder = new StringObjectTripleEncoder()
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
    val encoder = new StringObjectTripleEncoder()
    val expected = StructType(Seq(
      StructField("subject", LongType, nullable = false),
      StructField("predicate", StringType),
      StructField("objectString", StringType),
      StructField("objectType", StringType)
    ))
    assert(encoder.readSchema() === expected)
  }

  it("should provide the expected schema") {
    val encoder = new StringObjectTripleEncoder()
    val expected = StructType(Seq(
      StructField("subject", LongType, nullable = false),
      StructField("predicate", StringType),
      StructField("objectString", StringType),
      StructField("objectType", StringType)
    ))
    assert(encoder.schema() === expected)
  }

}
