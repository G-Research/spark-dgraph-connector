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
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import uk.co.gresearch.spark.dgraph.connector.{TypedNode, Geo, Password, Triple, TriplesFactory}

/**
 * Encodes only triples that represent nodes, i.e. object is not a uid.
 */
case class TypedNodeEncoder(triplesFactory: TriplesFactory) extends TripleEncoder {

  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an
   * empty schema can be returned here.
   * From: org.apache.spark.sql.connector.catalog.Table.schema
   */
  override def schema(): StructType = Encoders.product[TypedNode].schema

  /**
   * Returns the actual schema of this data source scan, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   * From: org.apache.spark.sql.connector.read.Scan.readSchema
   */
  override def readSchema(): StructType = schema()

  /**
   * Encodes a triple as an InternalRow.
   *
   * @param triple a Triple
   * @return an InternalRow
   */
  override def asInternalRow(triple: Triple): InternalRow = {
    val objectType = triplesFactory.getType(triple.o)

    if (objectType == "uid")
      throw new IllegalArgumentException(s"Node triple expected with object not being a uid: " +
        s"Triple(" +
        s"${triple.s}: ${triplesFactory.getType(triple.s)}, " +
        s"${triple.p}: ${triplesFactory.getType(triple.p)}, " +
        s"${triple.o}: ${triplesFactory.getType(triple.o)}" +
        s")"
      )

    // order has to align with TypedNode case class
    val valuesWithoutObject = Seq(
      triple.s.uid,
      UTF8String.fromString(triple.p),
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
        case "string" => (2, UTF8String.fromString(triple.o.asInstanceOf[String]))
        case "long" => (3, triple.o)
        case "double" => (4, triple.o)
        case "timestamp" => (5, DateTimeUtils.fromJavaTimestamp(triple.o.asInstanceOf[Timestamp]))
        case "boolean" => (6, triple.o)
        case "geo" => (7, UTF8String.fromString(triple.o.asInstanceOf[Geo].geo))
        case "password" => (8, UTF8String.fromString(triple.o.asInstanceOf[Password].password))
        case "default" => (2, UTF8String.fromString(triple.o.toString))
        case _ => (2, UTF8String.fromString(triple.o.toString))
      }
    val values = valuesWithoutObject.updated(objectValueIndex, objectValue)

    InternalRow(values: _*)
  }

}
