package uk.co.gresearch.spark.dgraph.connector.encoder

import java.sql.Timestamp

import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.{Password, Triple, Uid}

class TestStringObjectTripleEncoder extends FunSpec {

  describe("StringObjectTripleEncoder") {
    it("should encode edges to internalrow") {
      val encoder = new StringObjectTripleEncoder()
      val edge = Triple(Uid(1), "predicate", Uid(2))
      val row = encoder.asInternalRow(edge)
      assert(row.numFields === 4)
      assert(row.getLong(0) === 1)
      assert(row.getString(1) === "predicate")
      assert(row.getString(2) === "2")
      assert(row.getString(3) === "uid")
    }

    Seq(
      ("value", "value", "string", "string"),
      (123, "123", "int", "int"),
      (123L, "123", "int", "long"),
      (123.456f, "123.456", "float", "float"),
      (123.456, "123.456", "float", "double"),
      (Timestamp.valueOf("2020-01-02 12:34:56.789"), "2020-01-02 12:34:56.789", "dateTime", "dateTime"),
      (true, "true", "bool", "boolean"),
      (Uid(1), "1", "uid", "uid"),
      // TODO: test geo value
      (Password("secret"), "secret", "password", "password"),
      (new Object() {
        override def toString: String = "object"}, "object", "default", "default"),
    ).foreach { case (value, encoded, encType, test) =>
      it(s"should encode $test properties to internalrow") {
        val encoder = new StringObjectTripleEncoder()
        val edge = Triple(Uid(1), "predicate", value)
        val row = encoder.asInternalRow(edge)
        assert(row.numFields === 4)
        assert(row.getLong(0) === 1)
        assert(row.getString(1) === "predicate")
        assert(row.getString(2) === encoded)
        assert(row.getString(3) === encType)
      }
    }
  }
}
