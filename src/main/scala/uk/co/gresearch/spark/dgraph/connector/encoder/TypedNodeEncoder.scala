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

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import uk.co.gresearch.spark.dgraph.connector.{Geo, Password, Predicate, TypedNode, Uid}

/**
 * Encodes only triples that represent nodes, i.e. object is not a uid.
 */
case class TypedNodeEncoder(predicates: Map[String, Predicate]) extends TripleEncoder with ColumnInfoProvider {

  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an empty schema can be
   * returned here. From: org.apache.spark.sql.connector.catalog.Table.schema
   */
  override def schema(): StructType = TypedNodeEncoder.schema

  /**
   * Returns the actual schema of this data source scan, which may be different from the physical schema of the
   * underlying storage, as column pruning or other optimizations may happen. From:
   * org.apache.spark.sql.connector.read.Scan.readSchema
   */
  override def readSchema(): StructType = schema()

  override val subjectColumnName: Option[String] = Some(schema().fields.head.name)
  override val predicateColumnName: Option[String] = Some(schema().fields(1).name)
  override val objectTypeColumnName: Option[String] = Some(schema().fields.last.name)
  override val objectValueColumnNames: Option[Set[String]] =
    Some(schema().fields.drop(2).dropRight(1).map(_.name).toSet)
  override val objectTypes: Option[Map[String, String]] =
    Some(
      schema().fields
        .map(_.name)
        .filter(f => f.startsWith("object") && !f.equals("objectType"))
        .map(f => f -> f.substring(6).toLowerCase())
        .toMap
    )

  override def isPredicateValueColumn(columnName: String): Boolean = false

  /**
   * Encodes a triple (s, p, o) as an internal row. Returns None if triple cannot be encoded.
   *
   * @param s
   *   subject
   * @param p
   *   predicate
   * @param o
   *   object
   * @return
   *   an internal row
   */
  def asInternalRow(s: Uid, p: String, o: Any): Option[InternalRow] = {
    val objectType = getType(o)

    if (objectType == "uid") {
      None
    } else {
      // order has to align with TypedNode case class
      val valuesWithoutObject = Seq(
        s.uid.longValue(),
        UTF8String.fromString(p),
        null, // string
        null, // long
        null, // double
        null, // timestamp
        null, // boolean
        null, // geo
        null, // password
        UTF8String.fromString(objectType)
      )

      // order has to align with TypedNode case class
      val (objectValueIndex, objectValue) =
        objectType match {
          case "string"    => (2, UTF8String.fromString(o.asInstanceOf[String]))
          case "long"      => (3, o)
          case "double"    => (4, o)
          case "timestamp" => (5, DateTimeUtils.fromJavaTimestamp(o.asInstanceOf[Timestamp]))
          case "boolean"   => (6, o)
          case "geo"       => (7, UTF8String.fromString(o.asInstanceOf[Geo].geo))
          case "password"  => (8, UTF8String.fromString(o.asInstanceOf[Password].password))
          case "default"   => (2, UTF8String.fromString(o.toString))
          case _           => (2, UTF8String.fromString(o.toString))
        }
      val values = valuesWithoutObject.updated(objectValueIndex, objectValue)

      Some(InternalRow(values: _*))
    }
  }

}

object TypedNodeEncoder {
  private val fields = Encoders.product[TypedNode].schema.fields
  private val nullableIdx = (2 to (fields.length - 2)).toSet
  val schema: StructType = StructType(fields.zipWithIndex.map { case (field, idx) =>
    if (nullableIdx.contains(idx)) field.copy(nullable = true) else field.copy(nullable = false)
  })
}
