package uk.co.gresearch.spark.dgraph.connector

import java.sql.Timestamp

import org.apache.spark.sql
import org.apache.spark.sql.sources.{EqualTo, In}
import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.encoder.ColumnInfoProvider

class TestFilterTranslator extends FunSpec {

  // typed triples
  val subjectColumn = "subject"
  val predicateColumn = "predicate"
  val objectUidColumn = "objectUid"
  val objectStringColumn = "objectString"
  val objectLongColumn = "objectLong"
  val objectDoubleColumn = "objectDouble"
  val objectTimestampColumn = "objectTimestamp"
  val objectBooleanColumn = "objectBoolean"
  val objectGeoColumn = "objectGeo"
  val objectPasswordColumn = "objectPassword"
  val objectTypeColumn = "objectType"

  // string triples
  val allObjectStringColumn = "all object' string value"

  // wide nodes
  val predicateValueColumn = "a predicate value"

  val objectValueTypes = Map(
    objectUidColumn -> "uid",
    objectStringColumn -> "string",
    objectLongColumn -> "long",
    objectDoubleColumn -> "double",
    objectTimestampColumn -> "timestamp",
    objectBooleanColumn -> "boolean",
    objectGeoColumn -> "geo",
    objectPasswordColumn -> "password"
  )
  val objectValues: Set[String] = objectValueTypes.keys.toSet ++ Set(allObjectStringColumn)

  val columns: ColumnInfoProvider = new ColumnInfoProvider {
    override def subjectColumnName: Option[String] = Some(subjectColumn)
    override def predicateColumnName: Option[String] = Some(predicateColumn)
    override def isPredicateValueColumn(columnName: String): Boolean = columnName.equals(predicateValueColumn)
    override def objectTypeColumnName: Option[String] = Some(objectTypeColumn)
    override def objectValueColumnNames: Option[Set[String]] = Some(objectValues)
    override def objectTypes: Option[Map[String, String]] = Some(objectValueTypes)
  }

  val translator: FilterTranslator = FilterTranslator(columns)

  val timestamp: Timestamp = Timestamp.valueOf("2020-01-01 12:34:56")

  describe("FilterTranslator") {

    def doTest(filter: sql.sources.Filter, expected: Option[Seq[Filter]]): Unit = {
      val actual = translator.translate(filter)
      assert(actual === expected)
    }

    it("should translate EqualTo") {
      doTest(EqualTo(subjectColumn, 1), Some(List(SubjectIsIn(Uid(1)))))
      doTest(EqualTo(predicateColumn, "val"), Some(Seq(PredicateNameIsIn("val"))))
      doTest(EqualTo(predicateValueColumn, "val"), Some(List(PredicateNameIsIn(predicateValueColumn), ObjectValueIsIn("val"))))

      doTest(EqualTo(objectTypeColumn, "type"), Some(Seq(ObjectTypeIsIn("type"))))

      doTest(EqualTo(objectUidColumn, 1L), Some(Seq(ObjectValueIsIn("1"), ObjectTypeIsIn("uid"))))
      doTest(EqualTo(objectStringColumn, "val"), Some(Seq(ObjectValueIsIn("val"), ObjectTypeIsIn("string"))))
      doTest(EqualTo(objectLongColumn, 1L), Some(Seq(ObjectValueIsIn("1"), ObjectTypeIsIn("long"))))
      doTest(EqualTo(objectDoubleColumn, 1.0), Some(Seq(ObjectValueIsIn("1.0"), ObjectTypeIsIn("double"))))
      doTest(EqualTo(objectTimestampColumn, timestamp), Some(Seq(ObjectValueIsIn(timestamp.toString), ObjectTypeIsIn("timestamp"))))
      doTest(EqualTo(objectBooleanColumn, true), Some(Seq(ObjectValueIsIn("true"), ObjectTypeIsIn("boolean"))))
      doTest(EqualTo(objectGeoColumn, "geo"), Some(Seq(ObjectValueIsIn("geo"), ObjectTypeIsIn("geo"))))
      doTest(EqualTo(objectPasswordColumn, "pass"), Some(Seq(ObjectValueIsIn("pass"), ObjectTypeIsIn("password"))))

      doTest(EqualTo(allObjectStringColumn, "val"), None)
    }

    it("should translate In") {
      doTest(In(subjectColumn, Array(1)), Some(List(SubjectIsIn(Uid(1)))))
      doTest(In(predicateColumn, Array("val")), Some(Seq(PredicateNameIsIn("val"))))
      doTest(In(predicateValueColumn, Array("val")), Some(List(PredicateNameIsIn(predicateValueColumn), ObjectValueIsIn("val"))))

      doTest(In(objectTypeColumn, Array("type")), Some(Seq(ObjectTypeIsIn("type"))))

      doTest(In(objectUidColumn, Array(1L)), Some(Seq(ObjectValueIsIn("1"), ObjectTypeIsIn("uid"))))
      doTest(In(objectStringColumn, Array("val")), Some(Seq(ObjectValueIsIn("val"), ObjectTypeIsIn("string"))))
      doTest(In(objectLongColumn, Array(1L)), Some(Seq(ObjectValueIsIn("1"), ObjectTypeIsIn("long"))))
      doTest(In(objectDoubleColumn, Array(1.0)), Some(Seq(ObjectValueIsIn("1.0"), ObjectTypeIsIn("double"))))
      doTest(In(objectTimestampColumn, Array(timestamp)), Some(Seq(ObjectValueIsIn(timestamp.toString), ObjectTypeIsIn("timestamp"))))
      doTest(In(objectBooleanColumn, Array(true)), Some(Seq(ObjectValueIsIn("true"), ObjectTypeIsIn("boolean"))))
      doTest(In(objectGeoColumn, Array("geo")), Some(Seq(ObjectValueIsIn("geo"), ObjectTypeIsIn("geo"))))
      doTest(In(objectPasswordColumn, Array("pass")), Some(Seq(ObjectValueIsIn("pass"), ObjectTypeIsIn("password"))))

      doTest(In(allObjectStringColumn, Array("val")), None)
    }

  }

}
