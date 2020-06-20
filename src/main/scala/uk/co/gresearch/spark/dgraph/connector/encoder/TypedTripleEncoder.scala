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

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import uk.co.gresearch.spark.dgraph.connector.{Geo, Password, Predicate, TypedTriple, Uid}

/**
 * Encodes Triple by representing objects in multiple typed columns.
 **/
case class TypedTripleEncoder(predicates: Map[String, Predicate]) extends TripleEncoder {

  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an
   * empty schema can be returned here.
   * From: org.apache.spark.sql.connector.catalog.Table.schema
   */
  override def schema(): StructType = TypedTripleEncoder.schema()

  /**
   * Returns the actual schema of this data source scan, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   * From: org.apache.spark.sql.connector.read.Scan.readSchema
   */
  override def readSchema(): StructType = schema()

  /**
   * Encodes a triple (s, p, o) as an internal row. Returns None if triple cannot be encoded.
   *
   * @param s subject
   * @param p predicate
   * @param o object
   * @return an internal row
   */
  override def asInternalRow(s: Uid, p: String, o: Any): Option[InternalRow] = {
    val objectType = getType(o)

    // order has to align with TypedTriple
    val valuesWithoutObject = Seq(
      s.uid,
      UTF8String.fromString(p),
      null, // uid
      null, // string
      null, // long
      null, // double
      null, // timestamp
      null, // boolean
      null, // geo
      null, // password
      UTF8String.fromString(objectType)
    )

    // order has to align with TypedTriple
    val (objectValueIndex, objectValue) =
      objectType match {
        case "uid" => (2, o.asInstanceOf[Uid].uid)
        case "string" => (3, UTF8String.fromString(o.asInstanceOf[String]))
        case "long" => (4, o)
        case "double" => (5, o)
        case "timestamp" => (6, DateTimeUtils.fromJavaTimestamp(o.asInstanceOf[Timestamp]))
        case "boolean" => (7, o)
        case "geo" => (8, UTF8String.fromString(o.asInstanceOf[Geo].geo))
        case "password" => (9, UTF8String.fromString(o.asInstanceOf[Password].password))
        case "default" => (3, UTF8String.fromString(o.toString))
        case _ => (3, UTF8String.fromString(o.toString))
      }
    val values = valuesWithoutObject.updated(objectValueIndex, objectValue)

    Some(InternalRow(values: _*))
  }

}

object TypedTripleEncoder {
  def schema(): StructType = Encoders.product[TypedTriple].schema
}
