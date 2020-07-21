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

    describe("translate") {

      def testTranslate(filter: sql.sources.Filter, expected: Option[Seq[Filter]]): Unit = {
        val actual = translator.translate(filter)
        assert(actual === expected)
      }

      it("should translate IsNotNull") {
        testTranslate(IsNotNull(subjectColumn), None)

        testTranslate(IsNotNull(predicateColumn), None)

        testTranslate(IsNotNull(predicateValueColumn), Some(Seq(PredicateNameIs(predicateValueColumn))))

        testTranslate(IsNotNull(objectUidColumn), Some(Seq(ObjectTypeIsIn("uid"))))
        testTranslate(IsNotNull(objectStringColumn), Some(Seq(ObjectTypeIsIn("string"))))
        testTranslate(IsNotNull(objectLongColumn), Some(Seq(ObjectTypeIsIn("long"))))
        testTranslate(IsNotNull(objectDoubleColumn), Some(Seq(ObjectTypeIsIn("double"))))
        testTranslate(IsNotNull(objectTimestampColumn), Some(Seq(ObjectTypeIsIn("timestamp"))))
        testTranslate(IsNotNull(objectBooleanColumn), Some(Seq(ObjectTypeIsIn("boolean"))))
        testTranslate(IsNotNull(objectGeoColumn), Some(Seq(ObjectTypeIsIn("geo"))))
        testTranslate(IsNotNull(objectPasswordColumn), Some(Seq(ObjectTypeIsIn("password"))))

        testTranslate(IsNotNull(allObjectStringColumn), None)

        testTranslate(IsNotNull(objectTypeColumn), None)
      }

      it("should translate EqualTo") {
        testTranslate(EqualTo(subjectColumn, 1), Some(List(SubjectIsIn(Uid(1)))))

        testTranslate(EqualTo(predicateColumn, "val"), Some(Seq(IntersectPredicateNameIsIn("val"))))

        testTranslate(EqualTo(predicateValueColumn, "val"), Some(Seq(SinglePredicateValueIsIn(predicateValueColumn, Set("val")))))

        testTranslate(EqualTo(objectUidColumn, 1L), Some(Seq(ObjectValueIsIn(1L), ObjectTypeIsIn("uid"))))
        testTranslate(EqualTo(objectStringColumn, "val"), Some(Seq(ObjectValueIsIn("val"), ObjectTypeIsIn("string"))))
        testTranslate(EqualTo(objectLongColumn, 1L), Some(Seq(ObjectValueIsIn(1L), ObjectTypeIsIn("long"))))
        testTranslate(EqualTo(objectDoubleColumn, 1.0), Some(Seq(ObjectValueIsIn(1.0), ObjectTypeIsIn("double"))))
        testTranslate(EqualTo(objectTimestampColumn, timestamp), Some(Seq(ObjectValueIsIn(timestamp), ObjectTypeIsIn("timestamp"))))
        testTranslate(EqualTo(objectBooleanColumn, true), Some(Seq(ObjectValueIsIn(true), ObjectTypeIsIn("boolean"))))
        testTranslate(EqualTo(objectGeoColumn, "geo"), Some(Seq(ObjectValueIsIn("geo"), ObjectTypeIsIn("geo"))))
        testTranslate(EqualTo(objectPasswordColumn, "pass"), Some(Seq(ObjectValueIsIn("pass"), ObjectTypeIsIn("password"))))

        testTranslate(EqualTo(allObjectStringColumn, "val"), Some(Seq(ObjectValueIsIn("val"))))

        testTranslate(EqualTo(objectTypeColumn, "type"), Some(Seq(ObjectTypeIsIn("type"))))
      }

      it("should translate In") {
        testTranslate(In(subjectColumn, Array(1)), Some(List(SubjectIsIn(Uid(1)))))

        testTranslate(In(predicateColumn, Array("val")), Some(Seq(IntersectPredicateNameIsIn("val"))))

        testTranslate(In(predicateValueColumn, Array("val")), Some(Seq(SinglePredicateValueIsIn(predicateValueColumn, Set("val")))))

        testTranslate(In(objectUidColumn, Array(1L)), Some(Seq(ObjectValueIsIn(1L), ObjectTypeIsIn("uid"))))
        testTranslate(In(objectStringColumn, Array("val")), Some(Seq(ObjectValueIsIn("val"), ObjectTypeIsIn("string"))))
        testTranslate(In(objectLongColumn, Array(1L)), Some(Seq(ObjectValueIsIn(1L), ObjectTypeIsIn("long"))))
        testTranslate(In(objectDoubleColumn, Array(1.0)), Some(Seq(ObjectValueIsIn(1.0), ObjectTypeIsIn("double"))))
        testTranslate(In(objectTimestampColumn, Array(timestamp)), Some(Seq(ObjectValueIsIn(timestamp), ObjectTypeIsIn("timestamp"))))
        testTranslate(In(objectBooleanColumn, Array(true)), Some(Seq(ObjectValueIsIn(true), ObjectTypeIsIn("boolean"))))
        testTranslate(In(objectGeoColumn, Array("geo")), Some(Seq(ObjectValueIsIn("geo"), ObjectTypeIsIn("geo"))))
        testTranslate(In(objectPasswordColumn, Array("pass")), Some(Seq(ObjectValueIsIn("pass"), ObjectTypeIsIn("password"))))

        testTranslate(In(allObjectStringColumn, Array("val")), Some(Seq(ObjectValueIsIn("val"))))

        testTranslate(In(objectTypeColumn, Array("type")), Some(Seq(ObjectTypeIsIn("type"))))
      }

      it("should translate AlwaysTrue") {
        testTranslate(sql.sources.AlwaysTrue, Some(Seq(AlwaysTrue)))
      }

      it("should translate AlwaysFalse") {
        testTranslate(sql.sources.AlwaysFalse, Some(Seq(AlwaysFalse)))
      }

    }

    describe("simplify") {

      def testSimplify(filter: Seq[Filter], expected: Seq[Filter]): Unit = {
        val actual = FilterTranslator.simplify(filter)
        assert(actual === expected)
      }

      it("should simplify AlwaysTrue filters by deletion") {
        testSimplify(Seq(AlwaysTrue, AlwaysTrue, SubjectIsIn(Uid(1))), Seq(SubjectIsIn(Uid(1))))
      }

      it("should simplify duplicate filters") {
        Seq(
          AlwaysFalse,
          SubjectIsIn(Uid("0x1")),
          IntersectPredicateNameIsIn("a"),
          PredicateNameIs("a"),
          IntersectPredicateValueIsIn(Set("a"), Set("1")),
          SinglePredicateValueIsIn("a", Set("1")),
          ObjectTypeIsIn("a"),
          ObjectValueIsIn("a")
        ).foreach(filter => testSimplify(Seq(filter, filter), Seq(filter)))
      }

      it("should simplify filters having AlwaysFalse by single AlwaysFalse") {
        testSimplify(Seq(AlwaysTrue, AlwaysFalse, SubjectIsIn(Uid(1))), Seq(AlwaysFalse))
      }

      it("should simplify SubjectIsIn filters by intersection") {
        testSimplify(Seq(SubjectIsIn(Uid(1), Uid(2)), SubjectIsIn(Uid(2), Uid(3))), Seq(SubjectIsIn(Uid(2))))
      }

      describe("IntersectPredicateNameIsIn") {

        it("should be simplified with another IntersectPredicateNameIsIn to intersection") {
          testSimplify(Seq(IntersectPredicateNameIsIn("a", "b"), IntersectPredicateNameIsIn("b", "c")), Seq(IntersectPredicateNameIsIn(Set("b"))))
        }

        it("should be simplified with another IntersectPredicateNameIsIn to intersection and AlwaysFalse") {
          testSimplify(Seq(IntersectPredicateNameIsIn("a", "b"), IntersectPredicateNameIsIn("c", "d")), Seq(AlwaysFalse))
        }

        it("should be simplified with ObjectValueIsIn to IntersectPredicateValueIsIn") {
          testSimplify(Seq(IntersectPredicateNameIsIn("a", "b"), ObjectValueIsIn("c", "d")), Seq(IntersectPredicateValueIsIn(Set("a", "b"), Set("c", "d"))))
        }

        it("should be simplified with another IntersectPredicateNameIsIn and ObjectValueIsIn to intersection and IntersectPredicateValueIsIn") {
          testSimplify(Seq(IntersectPredicateNameIsIn("a", "b"), IntersectPredicateNameIsIn("b", "c"), ObjectValueIsIn("1", "2"), ObjectValueIsIn("2", "3")), Seq(IntersectPredicateValueIsIn(Set("b"), Set("2"))))
        }

        it("should be simplified with another IntersectPredicateNameIsIn and ObjectValueIsIn to intersection and AlwaysFalse") {
          testSimplify(Seq(IntersectPredicateNameIsIn("a", "b"), IntersectPredicateNameIsIn("c", "d"), ObjectValueIsIn("1", "2"), ObjectValueIsIn("3", "4")), Seq(AlwaysFalse))
        }

        it("should be simplified with PredicateValueIsIn to PredicateValueIsIn only") {
          {
            val valueIsIn = IntersectPredicateValueIsIn(Set("a"), Set("v"))
            testSimplify(Seq(IntersectPredicateNameIsIn("a"), valueIsIn), Seq(valueIsIn))
          }
          {
            val valueIsIn = SinglePredicateValueIsIn("a", Set("v"))
            testSimplify(Seq(IntersectPredicateNameIsIn("a"), valueIsIn), Seq(valueIsIn))
          }
        }

        it("should not be simplified with PredicateNameIs") {
          testSimplify(Seq(IntersectPredicateNameIsIn("a"), PredicateNameIs("a")), Seq(IntersectPredicateNameIsIn("a"), PredicateNameIs("a")))
          testSimplify(Seq(IntersectPredicateNameIsIn("a"), PredicateNameIs("b")), Seq(IntersectPredicateNameIsIn("a"), PredicateNameIs("b")))
        }

      }

      describe("IntersectPredicateValueIsIn") {

        it("should be simplified with another IntersectPredicateValueIsIn to intersection") {
          testSimplify(Seq(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), IntersectPredicateValueIsIn(Set("b", "c"), Set("2", "3"))), Seq(IntersectPredicateValueIsIn(Set("b"), Set("2"))))
        }

        it("should be simplified with another IntersectPredicateNameIsIn to intersection and AlwaysFalse") {
          testSimplify(Seq(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), IntersectPredicateValueIsIn(Set("b", "c"), Set("3", "4"))), Seq(AlwaysFalse))
          testSimplify(Seq(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), IntersectPredicateValueIsIn(Set("c", "d"), Set("2", "3"))), Seq(AlwaysFalse))
          testSimplify(Seq(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), IntersectPredicateValueIsIn(Set("c", "d"), Set("3", "4"))), Seq(AlwaysFalse))
        }

        it("should not be simplified with SinglePredicateValueIsIn") {
          testSimplify(Seq(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), SinglePredicateValueIsIn("b", Set("2", "3"))), Seq(SinglePredicateValueIsIn("b", Set("2", "3")), IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2"))))
          testSimplify(Seq(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), SinglePredicateValueIsIn("b", Set("3", "4"))), Seq(SinglePredicateValueIsIn("b", Set("3", "4")), IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2"))))
          testSimplify(Seq(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), SinglePredicateValueIsIn("c", Set("2", "3"))), Seq(SinglePredicateValueIsIn("c", Set("2", "3")), IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2"))))
        }

      }

      describe("PredicateNameIs") {

        it("should not be simplified with different PredicateNameIs") {
          testSimplify(Seq(PredicateNameIs("a"), PredicateNameIs("b")), Seq(PredicateNameIs("a"), PredicateNameIs("b")))
        }

        it("should be simplified with ObjectValueIsIn to SinglePredicateValueIsIn") {
          testSimplify(Seq(PredicateNameIs("a"), ObjectValueIsIn("c", "d")), Seq(SinglePredicateValueIsIn("a", Set("c", "d"))))
        }

        it("should be simplified with PredicateValueIsIn to PredicateValueIsIn only") {
          {
            val valueIsIn = IntersectPredicateValueIsIn(Set("a"), Set("v"))
            testSimplify(Seq(PredicateNameIs("a"), valueIsIn), Seq(valueIsIn))
          }
          {
            val valueIsIn = SinglePredicateValueIsIn("a", Set("v"))
            testSimplify(Seq(PredicateNameIs("a"), valueIsIn), Seq(valueIsIn))
          }
        }

      }

      describe("SinglePredicateValueIsIn") {

        it("should be simplified with same predicate SinglePredicateValueIsIn by value intersection") {
          testSimplify(Seq(SinglePredicateValueIsIn("a", Set("1", "2")), SinglePredicateValueIsIn("a", Set("2", "3"))), Seq(SinglePredicateValueIsIn("a", Set("2"))))
        }

        it("should not be simplified with different SinglePredicateValueIsIn") {
          testSimplify(Seq(SinglePredicateValueIsIn("a", Set("1", "2")), SinglePredicateValueIsIn("b", Set("2", "3"))), Seq(SinglePredicateValueIsIn("b", Set("2", "3")), SinglePredicateValueIsIn("a", Set("1", "2"))))
        }

      }

      it("should simplify ObjectTypeIsIn filters by intersection") {
        testSimplify(Seq(ObjectTypeIsIn("a", "b"), ObjectTypeIsIn("b", "c")), Seq(ObjectTypeIsIn("b")))
      }

      it("should simplify ObjectValueIsIn filters by intersection") {
        testSimplify(Seq(ObjectValueIsIn("a", "b"), ObjectValueIsIn("b", "c")), Seq(ObjectValueIsIn("b")))
      }

      describe("with supported") {

        def testSimplify(filter: Filters, supported: Seq[Filter] => Boolean, expected: Filters): Unit = {
          val actual = FilterTranslator.simplify(filter, supported)
          assert(actual === expected)
        }

        it("should simplify promised with optional only when simplification is supported") {
          def supported(filters: Seq[Filter]): Boolean = true

          def notSupported(filters: Seq[Filter]): Boolean = {
            !filters.contains(AlwaysFalse)
          }

          // PredicateNameIsIn is required, its simplification is not supported
          val filters = Filters.from(Seq(IntersectPredicateNameIsIn("a")), Seq(IntersectPredicateNameIsIn("b")))
          testSimplify(filters, notSupported, filters)

          // contradicting PredicateNameIsIn simplify to AlwaysFalse, when it is supported
          testSimplify(filters, supported, Filters.fromPromised(AlwaysFalse))
        }

        it("should simplify promised only when simplification is supported") {
          def supported(filters: Seq[Filter]): Boolean = true

          def notSupported(filters: Seq[Filter]): Boolean = {
            !filters.contains(AlwaysFalse)
          }

          // PredicateNameIsIn is required, its simplification is not supported
          val filters = Filters.fromPromised(IntersectPredicateNameIsIn("a"), IntersectPredicateNameIsIn("b"))
          testSimplify(filters, notSupported, filters)

          // contradicting PredicateNameIsIn simplify to AlwaysFalse, when it is supported
          testSimplify(filters, supported, Filters.fromPromised(AlwaysFalse))
        }

        it("should simplify optional only when simplification is supported") {
          def supported(filters: Seq[Filter]): Boolean = true

          def notSupported(filters: Seq[Filter]): Boolean = {
            !filters.contains(AlwaysFalse)
          }

          // PredicateNameIsIn is required, its simplification is not supported
          val filters = Filters.fromOptional(IntersectPredicateNameIsIn("a"), IntersectPredicateNameIsIn("b"))
          testSimplify(filters, notSupported, filters)

          // contradicting PredicateNameIsIn simplify to AlwaysFalse, when it is supported
          testSimplify(filters, supported, Filters.fromOptional(AlwaysFalse))
        }

      }
    }
  }

}
