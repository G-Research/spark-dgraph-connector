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

package uk.co.gresearch.spark.dgraph.connector

import java.sql.Timestamp

import org.apache.spark.sql
import org.apache.spark.sql.sources._
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.connector.encoder.ColumnInfoProvider

class TestFilterTranslator extends AnyFunSpec {

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
  val objectValues: Set[String] = objectValueTypes.keys.toSet + allObjectStringColumn

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

    it("should remove backticks from column name") {
      assert(ColumnName.unapply("name") === Some("name"))
      assert(ColumnName.unapply("`name`") === Some("name"))
      assert(ColumnName.unapply("`dgraph.type`") === Some("dgraph.type"))
      assert(ColumnName.unapply("`name") === Some("`name"))
      assert(ColumnName.unapply("name`") === Some("name`"))
      assert(ColumnName.unapply("dgraph`type") === Some("dgraph`type"))
      assert(ColumnName.unapply("`dgraph`type`") === Some("dgraph`type"))
    }

    describe("translate") {

      def testTranslate(filter: sql.sources.Filter, expected: Option[Set[Filter]]): Unit = {
        val actual = translator.translate(filter)
        assert(actual === expected)
      }

      it("should translate IsNotNull") {
        testTranslate(IsNotNull(subjectColumn), None)

        testTranslate(IsNotNull(predicateColumn), None)

        testTranslate(IsNotNull(predicateValueColumn), Some(Set(PredicateNameIs(predicateValueColumn))))

        testTranslate(IsNotNull(objectUidColumn), Some(Set(ObjectTypeIsIn("uid"))))
        testTranslate(IsNotNull(objectStringColumn), Some(Set(ObjectTypeIsIn("string"))))
        testTranslate(IsNotNull(objectLongColumn), Some(Set(ObjectTypeIsIn("long"))))
        testTranslate(IsNotNull(objectDoubleColumn), Some(Set(ObjectTypeIsIn("double"))))
        testTranslate(IsNotNull(objectTimestampColumn), Some(Set(ObjectTypeIsIn("timestamp"))))
        testTranslate(IsNotNull(objectBooleanColumn), Some(Set(ObjectTypeIsIn("boolean"))))
        testTranslate(IsNotNull(objectGeoColumn), Some(Set(ObjectTypeIsIn("geo"))))
        testTranslate(IsNotNull(objectPasswordColumn), Some(Set(ObjectTypeIsIn("password"))))

        testTranslate(IsNotNull(allObjectStringColumn), None)

        testTranslate(IsNotNull(objectTypeColumn), None)
      }

      it("should translate EqualTo") {
        testTranslate(EqualTo(subjectColumn, 1), Some(Set(SubjectIsIn(Uid(1)))))

        testTranslate(EqualTo(predicateColumn, "val"), Some(Set(IntersectPredicateNameIsIn("val"))))

        testTranslate(EqualTo(predicateValueColumn, "val"), Some(Set(SinglePredicateValueIsIn(predicateValueColumn, Set("val")))))

        testTranslate(EqualTo(objectUidColumn, 1L), Some(Set(ObjectValueIsIn(1L), ObjectTypeIsIn("uid"))))
        testTranslate(EqualTo(objectStringColumn, "val"), Some(Set(ObjectValueIsIn("val"), ObjectTypeIsIn("string"))))
        testTranslate(EqualTo(objectLongColumn, 1L), Some(Set(ObjectValueIsIn(1L), ObjectTypeIsIn("long"))))
        testTranslate(EqualTo(objectDoubleColumn, 1.0), Some(Set(ObjectValueIsIn(1.0), ObjectTypeIsIn("double"))))
        testTranslate(EqualTo(objectTimestampColumn, timestamp), Some(Set(ObjectValueIsIn(timestamp), ObjectTypeIsIn("timestamp"))))
        testTranslate(EqualTo(objectBooleanColumn, true), Some(Set(ObjectValueIsIn(true), ObjectTypeIsIn("boolean"))))
        testTranslate(EqualTo(objectGeoColumn, "geo"), Some(Set(ObjectValueIsIn("geo"), ObjectTypeIsIn("geo"))))
        testTranslate(EqualTo(objectPasswordColumn, "pass"), Some(Set(ObjectValueIsIn("pass"), ObjectTypeIsIn("password"))))

        testTranslate(EqualTo(allObjectStringColumn, "val"), Some(Set(ObjectValueIsIn("val"))))

        testTranslate(EqualTo(objectTypeColumn, "type"), Some(Set(ObjectTypeIsIn("type"))))
      }

      it("should translate In") {
        testTranslate(In(subjectColumn, Array(1)), Some(Set(SubjectIsIn(Uid(1)))))

        testTranslate(In(predicateColumn, Array("val")), Some(Set(IntersectPredicateNameIsIn("val"))))

        testTranslate(In(predicateValueColumn, Array("val")), Some(Set(SinglePredicateValueIsIn(predicateValueColumn, Set("val")))))

        testTranslate(In(objectUidColumn, Array(1L)), Some(Set(ObjectValueIsIn(1L), ObjectTypeIsIn("uid"))))
        testTranslate(In(objectStringColumn, Array("val")), Some(Set(ObjectValueIsIn("val"), ObjectTypeIsIn("string"))))
        testTranslate(In(objectLongColumn, Array(1L)), Some(Set(ObjectValueIsIn(1L), ObjectTypeIsIn("long"))))
        testTranslate(In(objectDoubleColumn, Array(1.0)), Some(Set(ObjectValueIsIn(1.0), ObjectTypeIsIn("double"))))
        testTranslate(In(objectTimestampColumn, Array(timestamp)), Some(Set(ObjectValueIsIn(timestamp), ObjectTypeIsIn("timestamp"))))
        testTranslate(In(objectBooleanColumn, Array(true)), Some(Set(ObjectValueIsIn(true), ObjectTypeIsIn("boolean"))))
        testTranslate(In(objectGeoColumn, Array("geo")), Some(Set(ObjectValueIsIn("geo"), ObjectTypeIsIn("geo"))))
        testTranslate(In(objectPasswordColumn, Array("pass")), Some(Set(ObjectValueIsIn("pass"), ObjectTypeIsIn("password"))))

        testTranslate(In(allObjectStringColumn, Array("val")), Some(Set(ObjectValueIsIn("val"))))

        testTranslate(In(objectTypeColumn, Array("type")), Some(Set(ObjectTypeIsIn("type"))))
      }

      it("should translate AlwaysTrue") {
        testTranslate(sql.sources.AlwaysTrue, Some(Set(AlwaysTrue)))
      }

      it("should translate AlwaysFalse") {
        testTranslate(sql.sources.AlwaysFalse, Some(Set(AlwaysFalse)))
      }

      it("should translate backticked column names") {
        def backtick(columnName: String): String = s"`$columnName`"

        testTranslate(IsNotNull(backtick(predicateValueColumn)), Some(Set(PredicateNameIs(predicateValueColumn))))
        testTranslate(IsNotNull(backtick(objectStringColumn)), Some(Set(ObjectTypeIsIn("string"))))

        testTranslate(EqualTo(backtick(predicateColumn), "val"), Some(Set(IntersectPredicateNameIsIn("val"))))
        testTranslate(EqualTo(backtick(predicateValueColumn), "val"), Some(Set(SinglePredicateValueIsIn(predicateValueColumn, Set("val")))))
        testTranslate(EqualTo(backtick(objectStringColumn), "val"), Some(Set(ObjectValueIsIn("val"), ObjectTypeIsIn("string"))))
        testTranslate(EqualTo(backtick(allObjectStringColumn), "val"), Some(Set(ObjectValueIsIn("val"))))
        testTranslate(EqualTo(backtick(objectTypeColumn), "type"), Some(Set(ObjectTypeIsIn("type"))))

        testTranslate(In(backtick(subjectColumn), Array(1)), Some(Set(SubjectIsIn(Uid(1)))))
        testTranslate(In(backtick(predicateColumn), Array("val")), Some(Set(IntersectPredicateNameIsIn("val"))))
        testTranslate(In(backtick(predicateValueColumn), Array("val")), Some(Set(SinglePredicateValueIsIn(predicateValueColumn, Set("val")))))
        testTranslate(In(backtick(objectStringColumn), Array("val")), Some(Set(ObjectValueIsIn("val"), ObjectTypeIsIn("string"))))
        testTranslate(In(backtick(allObjectStringColumn), Array("val")), Some(Set(ObjectValueIsIn("val"))))
        testTranslate(In(backtick(objectTypeColumn), Array("type")), Some(Set(ObjectTypeIsIn("type"))))
      }

    }

    describe("simplify") {

      def testSimplify(filter: Set[Filter], expected: Set[Filter]): Unit = {
        val actual = FilterTranslator.simplify(filter)
        assert(actual === expected)
      }

      it("should simplify AlwaysTrue filters by deletion") {
        testSimplify(Set(AlwaysTrue, AlwaysTrue, SubjectIsIn(Uid(1))), Set(SubjectIsIn(Uid(1))))
      }

      it("should simplify filters having AlwaysFalse by single AlwaysFalse") {
        testSimplify(Set(AlwaysTrue, AlwaysFalse, SubjectIsIn(Uid(1))), Set(AlwaysFalse))
      }

      it("should simplify SubjectIsIn filters by intersection") {
        testSimplify(Set(SubjectIsIn(Uid(1), Uid(2)), SubjectIsIn(Uid(2), Uid(3))), Set(SubjectIsIn(Uid(2))))
      }

      describe("IntersectPredicateNameIsIn") {

        it("should be simplified with another IntersectPredicateNameIsIn to intersection") {
          testSimplify(Set(IntersectPredicateNameIsIn("a", "b"), IntersectPredicateNameIsIn("b", "c")), Set(IntersectPredicateNameIsIn(Set("b"))))
        }

        it("should be simplified with another IntersectPredicateNameIsIn to intersection and AlwaysFalse") {
          testSimplify(Set(IntersectPredicateNameIsIn("a", "b"), IntersectPredicateNameIsIn("c", "d")), Set(AlwaysFalse))
        }

        it("should be simplified with ObjectValueIsIn to IntersectPredicateValueIsIn") {
          testSimplify(Set(IntersectPredicateNameIsIn("a", "b"), ObjectValueIsIn("c", "d")), Set(IntersectPredicateValueIsIn(Set("a", "b"), Set("c", "d"))))
        }

        it("should be simplified with another IntersectPredicateNameIsIn and ObjectValueIsIn to intersection and IntersectPredicateValueIsIn") {
          testSimplify(Set(IntersectPredicateNameIsIn("a", "b"), IntersectPredicateNameIsIn("b", "c"), ObjectValueIsIn("1", "2"), ObjectValueIsIn("2", "3")), Set(IntersectPredicateValueIsIn(Set("b"), Set("2"))))
        }

        it("should be simplified with another IntersectPredicateNameIsIn and ObjectValueIsIn to intersection and AlwaysFalse") {
          testSimplify(Set(IntersectPredicateNameIsIn("a", "b"), IntersectPredicateNameIsIn("c", "d"), ObjectValueIsIn("1", "2"), ObjectValueIsIn("3", "4")), Set(AlwaysFalse))
        }

        it("should be simplified with PredicateValueIsIn to PredicateValueIsIn only") {
          {
            val valueIsIn = IntersectPredicateValueIsIn(Set("a"), Set("v"))
            testSimplify(Set(IntersectPredicateNameIsIn("a"), valueIsIn), Set(valueIsIn))
          }
          {
            val valueIsIn = SinglePredicateValueIsIn("a", Set("v"))
            testSimplify(Set(IntersectPredicateNameIsIn("a"), valueIsIn), Set(valueIsIn))
          }
        }

        it("should not be simplified with PredicateNameIs") {
          testSimplify(Set(IntersectPredicateNameIsIn("a"), PredicateNameIs("a")), Set(IntersectPredicateNameIsIn("a"), PredicateNameIs("a")))
          testSimplify(Set(IntersectPredicateNameIsIn("a"), PredicateNameIs("b")), Set(IntersectPredicateNameIsIn("a"), PredicateNameIs("b")))
        }

      }

      describe("IntersectPredicateValueIsIn") {

        it("should be simplified with another IntersectPredicateValueIsIn to intersection") {
          testSimplify(Set(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), IntersectPredicateValueIsIn(Set("b", "c"), Set("2", "3"))), Set(IntersectPredicateValueIsIn(Set("b"), Set("2"))))
        }

        it("should be simplified with another IntersectPredicateNameIsIn to intersection and AlwaysFalse") {
          testSimplify(Set(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), IntersectPredicateValueIsIn(Set("b", "c"), Set("3", "4"))), Set(AlwaysFalse))
          testSimplify(Set(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), IntersectPredicateValueIsIn(Set("c", "d"), Set("2", "3"))), Set(AlwaysFalse))
          testSimplify(Set(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), IntersectPredicateValueIsIn(Set("c", "d"), Set("3", "4"))), Set(AlwaysFalse))
        }

        it("should not be simplified with SinglePredicateValueIsIn") {
          testSimplify(Set(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), SinglePredicateValueIsIn("b", Set("2", "3"))), Set(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), SinglePredicateValueIsIn("b", Set("2", "3"))))
          testSimplify(Set(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), SinglePredicateValueIsIn("b", Set("3", "4"))), Set(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), SinglePredicateValueIsIn("b", Set("3", "4"))))
          testSimplify(Set(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), SinglePredicateValueIsIn("c", Set("2", "3"))), Set(IntersectPredicateValueIsIn(Set("a", "b"), Set("1", "2")), SinglePredicateValueIsIn("c", Set("2", "3"))))
        }

      }

      describe("PredicateNameIs") {

        it("should not be simplified with different PredicateNameIs") {
          testSimplify(Set(PredicateNameIs("a"), PredicateNameIs("b")), Set(PredicateNameIs("a"), PredicateNameIs("b")))
        }

        it("should be simplified with ObjectValueIsIn to SinglePredicateValueIsIn") {
          testSimplify(Set(PredicateNameIs("a"), ObjectValueIsIn("c", "d")), Set(SinglePredicateValueIsIn("a", Set("c", "d"))))
        }

        it("should be simplified with PredicateValueIsIn to PredicateValueIsIn only") {
          {
            val valueIsIn = IntersectPredicateValueIsIn(Set("a"), Set("v"))
            testSimplify(Set(PredicateNameIs("a"), valueIsIn), Set(valueIsIn))
          }
          {
            val valueIsIn = SinglePredicateValueIsIn("a", Set("v"))
            testSimplify(Set(PredicateNameIs("a"), valueIsIn), Set(valueIsIn))
          }
        }

      }

      describe("SinglePredicateValueIsIn") {

        it("should be simplified with same predicate SinglePredicateValueIsIn by value intersection") {
          testSimplify(Set(SinglePredicateValueIsIn("a", Set("1", "2")), SinglePredicateValueIsIn("a", Set("2", "3"))), Set(SinglePredicateValueIsIn("a", Set("2"))))
        }

        it("should not be simplified with different SinglePredicateValueIsIn") {
          testSimplify(Set(SinglePredicateValueIsIn("a", Set("1", "2")), SinglePredicateValueIsIn("b", Set("2", "3"))), Set(SinglePredicateValueIsIn("a", Set("1", "2")), SinglePredicateValueIsIn("b", Set("2", "3"))))
        }

      }

      it("should simplify ObjectTypeIsIn filters by intersection") {
        testSimplify(Set(ObjectTypeIsIn("a", "b"), ObjectTypeIsIn("b", "c")), Set(ObjectTypeIsIn("b")))
      }

      it("should simplify ObjectValueIsIn filters by intersection") {
        testSimplify(Set(ObjectValueIsIn("a", "b"), ObjectValueIsIn("b", "c")), Set(ObjectValueIsIn("b")))
      }

      describe("with supported") {

        def testSimplify(filter: Filters, supported: Set[Filter] => Boolean, expected: Filters): Unit = {
          val actual = FilterTranslator.simplify(filter, supported)
          assert(actual === expected)
        }

        it("should simplify promised with optional only when simplification is supported") {
          def supported(filters: Set[Filter]): Boolean = true

          def notSupported(filters: Set[Filter]): Boolean = {
            !filters.contains(AlwaysFalse)
          }

          // PredicateNameIsIn is required, its simplification is not supported
          val filters = Filters.from(Set(IntersectPredicateNameIsIn("a")), Set(IntersectPredicateNameIsIn("b")))
          testSimplify(filters, notSupported, filters)

          // contradicting PredicateNameIsIn simplify to AlwaysFalse, when it is supported
          testSimplify(filters, supported, Filters.fromPromised(AlwaysFalse))
        }

        it("should simplify promised only when simplification is supported") {
          def supported(filters: Set[Filter]): Boolean = true

          def notSupported(filters: Set[Filter]): Boolean = {
            !filters.contains(AlwaysFalse)
          }

          // PredicateNameIsIn is required, its simplification is not supported
          val filters = Filters.fromPromised(IntersectPredicateNameIsIn("a"), IntersectPredicateNameIsIn("b"))
          testSimplify(filters, notSupported, filters)

          // contradicting PredicateNameIsIn simplify to AlwaysFalse, when it is supported
          testSimplify(filters, supported, Filters.fromPromised(AlwaysFalse))
        }

        it("should simplify optional only when simplification is supported") {
          def supported(filters: Set[Filter]): Boolean = true

          def notSupported(filters: Set[Filter]): Boolean = {
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
