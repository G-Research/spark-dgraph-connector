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

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.{Edge, Uid}

/**
 * Encodes only triples that represent edges, i.e. object is a uid.
 * Non-edges will be silently ignored.
 */
case class EdgeEncoder(predicates: Map[String, connector.Predicate])
  extends TripleEncoder with ColumnInfoProvider {

  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an
   * empty schema can be returned here.
   * From: org.apache.spark.sql.connector.catalog.Table.schema
   */
  override def schema(): StructType = EdgeEncoder.schema

  /**
   * Returns the actual schema of this data source scan, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   * From: org.apache.spark.sql.connector.read.Scan.readSchema
   */
  override def readSchema(): StructType = schema()

  override val subjectColumnName: Option[String] = Some(schema().fields.head.name)
  override val predicateColumnName: Option[String] = Some(schema().fields(1).name)
  override val objectTypeColumnName: Option[String] = None
  override val objectValueColumnNames: Option[Set[String]] = Some(Set(schema().fields.last.name))
  override val objectTypes: Option[Map[String, String]] = Some(Map(schema().fields.last.name -> "uid"))

  override def isPredicateValueColumn(columnName: String): Boolean = false

  /**
   * Encodes a triple (s, p, o) as an internal row. Returns None if triple cannot be encoded.
   *
   * @param s subject
   * @param p predicate
   * @param o object
   * @return an internal row
   */
  override def asInternalRow(s: Uid, p: String, o: Any): Option[InternalRow] = o match {
    case uid: Uid => Some(InternalRow(s.uid.longValue(), UTF8String.fromString(p), uid.uid.longValue()))
    case _ => None
  }

}

object EdgeEncoder {
  private val fields = Encoders.product[Edge].schema.fields
  val schema: StructType = StructType(fields.map(_.copy(nullable = false)))
}
