package uk.co.gresearch.spark.dgraph.connector

import java.sql.Timestamp

import org.apache.spark.sql
import org.apache.spark.sql.sources._
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
  val allObjectStringColumn = "all object values string"

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

    it("should translate IsNotNull") {
      doTest(IsNotNull(subjectColumn), None)
      doTest(IsNotNull(predicateColumn), None)

      doTest(IsNotNull(objectUidColumn), Some(Seq(ObjectTypeIsIn("uid"))))
      doTest(IsNotNull(objectStringColumn), Some(Seq(ObjectTypeIsIn("string"))))
      doTest(IsNotNull(objectLongColumn), Some(Seq(ObjectTypeIsIn("long"))))
      doTest(IsNotNull(objectDoubleColumn), Some(Seq(ObjectTypeIsIn("double"))))
      doTest(IsNotNull(objectTimestampColumn), Some(Seq(ObjectTypeIsIn("timestamp"))))
      doTest(IsNotNull(objectBooleanColumn), Some(Seq(ObjectTypeIsIn("boolean"))))
      doTest(IsNotNull(objectGeoColumn), Some(Seq(ObjectTypeIsIn("geo"))))
      doTest(IsNotNull(objectPasswordColumn), Some(Seq(ObjectTypeIsIn("password"))))

      doTest(IsNotNull(allObjectStringColumn), None)
    }

    it("should translate EqualTo") {
      doTest(EqualTo(subjectColumn, 1), Some(List(SubjectIsIn(Uid(1)))))
      doTest(EqualTo(predicateColumn, "val"), Some(Seq(PredicateNameIsIn("val"))))

      doTest(EqualTo(objectTypeColumn, "type"), Some(Seq(ObjectTypeIsIn("type"))))

      doTest(EqualTo(objectUidColumn, 1L), Some(Seq(ObjectValueIsIn(1L), ObjectTypeIsIn("uid"))))
      doTest(EqualTo(objectStringColumn, "val"), Some(Seq(ObjectValueIsIn("val"), ObjectTypeIsIn("string"))))
      doTest(EqualTo(objectLongColumn, 1L), Some(Seq(ObjectValueIsIn(1L), ObjectTypeIsIn("long"))))
      doTest(EqualTo(objectDoubleColumn, 1.0), Some(Seq(ObjectValueIsIn(1.0), ObjectTypeIsIn("double"))))
      doTest(EqualTo(objectTimestampColumn, timestamp), Some(Seq(ObjectValueIsIn(timestamp), ObjectTypeIsIn("timestamp"))))
      doTest(EqualTo(objectBooleanColumn, true), Some(Seq(ObjectValueIsIn(true), ObjectTypeIsIn("boolean"))))
      doTest(EqualTo(objectGeoColumn, "geo"), Some(Seq(ObjectValueIsIn("geo"), ObjectTypeIsIn("geo"))))
      doTest(EqualTo(objectPasswordColumn, "pass"), Some(Seq(ObjectValueIsIn("pass"), ObjectTypeIsIn("password"))))

      doTest(EqualTo(allObjectStringColumn, "val"), Some(Seq(ObjectValueIsIn("val"))))
    }

    it("should translate In") {
      doTest(In(subjectColumn, Array(1)), Some(List(SubjectIsIn(Uid(1)))))
      doTest(In(predicateColumn, Array("val")), Some(Seq(PredicateNameIsIn("val"))))

      doTest(In(objectTypeColumn, Array("type")), Some(Seq(ObjectTypeIsIn("type"))))

      doTest(In(objectUidColumn, Array(1L)), Some(Seq(ObjectValueIsIn(1L), ObjectTypeIsIn("uid"))))
      doTest(In(objectStringColumn, Array("val")), Some(Seq(ObjectValueIsIn("val"), ObjectTypeIsIn("string"))))
      doTest(In(objectLongColumn, Array(1L)), Some(Seq(ObjectValueIsIn(1L), ObjectTypeIsIn("long"))))
      doTest(In(objectDoubleColumn, Array(1.0)), Some(Seq(ObjectValueIsIn(1.0), ObjectTypeIsIn("double"))))
      doTest(In(objectTimestampColumn, Array(timestamp)), Some(Seq(ObjectValueIsIn(timestamp), ObjectTypeIsIn("timestamp"))))
      doTest(In(objectBooleanColumn, Array(true)), Some(Seq(ObjectValueIsIn(true), ObjectTypeIsIn("boolean"))))
      doTest(In(objectGeoColumn, Array("geo")), Some(Seq(ObjectValueIsIn("geo"), ObjectTypeIsIn("geo"))))
      doTest(In(objectPasswordColumn, Array("pass")), Some(Seq(ObjectValueIsIn("pass"), ObjectTypeIsIn("password"))))

      doTest(In(allObjectStringColumn, Array("val")), Some(Seq(ObjectValueIsIn("val"))))
    }

    it("should translate AlwaysTrue") {
      doTest(sql.sources.AlwaysTrue, Some(Seq(AlwaysTrue)))
    }

    it("should translate AlwaysFalse") {
      doTest(sql.sources.AlwaysFalse, Some(Seq(AlwaysFalse)))
    }

    it("should simplify AlwaysTrue filters by deletion") {
      val simplified = FilterTranslator.simplify(Seq(AlwaysTrue, AlwaysTrue, SubjectIsIn(Uid(1))))
      assert(simplified === Seq(SubjectIsIn(Uid(1))))
    }

    it("should simplify filters having AlwaysFalse by single AlwaysFalse") {
      val simplified = FilterTranslator.simplify(Seq(AlwaysTrue, AlwaysFalse, SubjectIsIn(Uid(1))))
      assert(simplified === Seq(AlwaysFalse))
    }

    it("should simplify SubjectIsIn filters by intersection") {
      val simplified = FilterTranslator.simplify(Seq(SubjectIsIn(Uid(1), Uid(2)), SubjectIsIn(Uid(2), Uid(3))))
      assert(simplified === Seq(SubjectIsIn(Uid(2))))
    }

    it("should simplify PredicateNameIsIn filters by intersection") {
      val simplified = FilterTranslator.simplify(Seq(PredicateNameIsIn("a", "b"), PredicateNameIsIn("b", "c")))
      assert(simplified === Seq(PredicateNameIsIn("b")))
    }

    it("should simplify ObjectTypeIsIn filters by intersection") {
      val simplified = FilterTranslator.simplify(Seq(ObjectTypeIsIn("a", "b"), ObjectTypeIsIn("b", "c")))
      assert(simplified === Seq(ObjectTypeIsIn("b")))
    }

    it("should simplify ObjectValueIsIn filters by intersection") {
      val simplified = FilterTranslator.simplify(Seq(ObjectValueIsIn("a", "b"), ObjectValueIsIn("b", "c")))
      assert(simplified === Seq(ObjectValueIsIn("b")))
    }

    describe("simplify Filters") {

      it("should simplify promised with optional only when simplification is supported") {
        def supported(filters: Seq[Filter]): Boolean = true

        def notSupported(filters: Seq[Filter]): Boolean = {
          !filters.contains(AlwaysFalse)
        }

        // PredicateNameIsIn is required, its simplification is not supported
        val filters = Filters.from(Seq(PredicateNameIsIn("a")), Seq(PredicateNameIsIn("b")))
        val simplified1 = FilterTranslator.simplify(filters, notSupported)
        assert(simplified1 === filters)

        // contradicting PredicateNameIsIn simplify to AlwaysFalse, when it is supported
        val simplified2 = FilterTranslator.simplify(filters, supported)
        assert(simplified2 === Filters.fromPromised(AlwaysFalse))
      }

      it("should simplify promised only when simplification is supported") {
        def supported(filters: Seq[Filter]): Boolean = true

        def notSupported(filters: Seq[Filter]): Boolean = {
          !filters.contains(AlwaysFalse)
        }

        // PredicateNameIsIn is required, its simplification is not supported
        val filters = Filters.fromPromised(PredicateNameIsIn("a"), PredicateNameIsIn("b"))
        val simplified1 = FilterTranslator.simplify(filters, notSupported)
        assert(simplified1 === filters)

        // contradicting PredicateNameIsIn simplify to AlwaysFalse, when it is supported
        val simplified2 = FilterTranslator.simplify(filters, supported)
        assert(simplified2 === Filters.fromPromised(AlwaysFalse))
      }

      it("should simplify optional only when simplification is supported") {
        def supported(filters: Seq[Filter]): Boolean = true

        def notSupported(filters: Seq[Filter]): Boolean = {
          !filters.contains(AlwaysFalse)
        }

        // PredicateNameIsIn is required, its simplification is not supported
        val filters = Filters.fromOptional(PredicateNameIsIn("a"), PredicateNameIsIn("b"))
        val simplified1 = FilterTranslator.simplify(filters, notSupported)
        assert(simplified1 === filters)

        // contradicting PredicateNameIsIn simplify to AlwaysFalse, when it is supported
        val simplified2 = FilterTranslator.simplify(filters, supported)
        assert(simplified2 === Filters.fromOptional(AlwaysFalse))
      }

    }
  }

}
