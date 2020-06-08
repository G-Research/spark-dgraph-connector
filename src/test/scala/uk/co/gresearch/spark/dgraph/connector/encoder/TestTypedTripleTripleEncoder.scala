package uk.co.gresearch.spark.dgraph.connector.encoder

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.{Geo, Password, Triple, Uid}

class TestTypedTripleTripleEncoder extends FunSpec {

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
      val encoder = TypedTripleEncoder()
      val triple = Triple(Uid(1), "predicate", value)
      val row = encoder.asInternalRow(triple)

      assert(row.numFields === 11)
      assert(row.getLong(0) === 1)
      assert(row.getString(1) === "predicate")
      if (encType == "uid") assert(row.getLong(2) === value.asInstanceOf[Uid].uid) else assert(row.get(2, StringType) === null)
      if (encType == "string" || encType == "default") assert(row.getUTF8String(3).toString === value.toString) else assert(row.get(3, StringType) === null)
      if (encType == "long") assert(row.getLong(4) === value) else assert(row.get(4, StringType) === null)
      if (encType == "double") assert(row.getDouble(5) === value) else assert(row.get(5, StringType) === null)
      if (encType == "timestamp") assert(row.get(6, TimestampType) === DateTimeUtils.fromJavaTimestamp(value.asInstanceOf[Timestamp])) else assert(row.get(6, StringType) === null)
      if (encType == "boolean") assert(row.getBoolean(7) === value) else assert(row.get(7, StringType) === null)
      if (encType == "geo") assert(row.getUTF8String(8).toString === value.asInstanceOf[Geo].geo) else assert(row.get(8, StringType) === null)
      if (encType == "password") assert(row.getUTF8String(9).toString === value.asInstanceOf[Password].password) else assert(row.get(9, StringType) === null)
      assert(row.getString(10) === encType)
    }

  }

  it("should provide the expected read schema") {
    val encoder = TypedTripleEncoder()
    val expected = StructType(Seq(
      StructField("subject", LongType, nullable = false),
      StructField("predicate", StringType),
      StructField("objectUid", LongType),
      StructField("objectString", StringType),
      StructField("objectLong", LongType),
      StructField("objectDouble", DoubleType),
      StructField("objectTimestamp", TimestampType),
      StructField("objectBoolean", BooleanType),
      StructField("objectGeo", StringType),
      StructField("objectPassword", StringType),
      StructField("objectType", StringType)
    ))
    assert(encoder.readSchema() === expected)
  }

  it("should provide the expected schema") {
    val encoder = TypedTripleEncoder()
    val expected = StructType(Seq(
      StructField("subject", LongType, nullable = false),
      StructField("predicate", StringType),
      StructField("objectUid", LongType),
      StructField("objectString", StringType),
      StructField("objectLong", LongType),
      StructField("objectDouble", DoubleType),
      StructField("objectTimestamp", TimestampType),
      StructField("objectBoolean", BooleanType),
      StructField("objectGeo", StringType),
      StructField("objectPassword", StringType),
      StructField("objectType", StringType)
    ))
    assert(encoder.schema() === expected)
  }

}
