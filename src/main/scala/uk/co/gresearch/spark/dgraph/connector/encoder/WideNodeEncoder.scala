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

import com.google.gson.{JsonArray, JsonObject}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import uk.co.gresearch.spark.dgraph.connector.{Geo, Json, Password, Predicate, Uid}

import scala.collection.JavaConverters._

/**
 * Encodes nodes as wide InternalRows from Dgraph json results.
 */
case class WideNodeEncoder(predicates: Map[String, Predicate]) extends JsonNodeInternalRowEncoder {

  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an
   * empty schema can be returned here.
   * From: org.apache.spark.sql.connector.catalog.Table.schema
   */
  override val schema: StructType = WideNodeEncoder.schema(predicates)

  /**
   * Returns the actual schema of this data source scan, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   * From: org.apache.spark.sql.connector.read.Scan.readSchema
   */
  override val readSchema: StructType = schema

  /**
   * 1-based dense column indices for predicate names.
   */
  val columns: Map[String, Int] = schema.fields.zipWithIndex.map{ case (p, i) => (p.name, i) }.drop(1).toMap

  /**
   * Encodes the given Dgraph json result into InternalRows.
   *
   * @param result Json result
   * @return internal rows
   */
  override def fromJson(result: JsonArray): Iterator[InternalRow] =
    getNodes(result).map(toNode)

  /**
   * Encodes a node as a wide InternalRow.
   *
   * @param node a json node to turn into a wide InternalRow
   * @return InternalRows
   */
  def toNode(node: JsonObject): InternalRow = {
    val uidString = node.remove("uid").getAsString
    val uid = Uid(uidString)

    val values = Array.fill[Any](columns.size + 1)(null)
    values(0) = uid.uid

    // put all values into corresponding columns of 'values'
    node.entrySet().iterator().asScala
      .map { e =>
        (
          columns.get(e.getKey),
          predicates.get(e.getKey).map(_.typeName),
          e.getValue,
        )
      }
      .filter( e => e._1.isDefined && e._2.isDefined )
      .foreach { case (Some(p), Some(t), o) =>
        val obj = getValue(o, t)
        val objectValue = t match {
          case "string" => UTF8String.fromString(obj.asInstanceOf[String])
          case "int" => obj
          case "float" => obj
          case "datetime" => DateTimeUtils.fromJavaTimestamp(obj.asInstanceOf[Timestamp])
          case "boolean" => obj
          case "geo" => UTF8String.fromString(obj.asInstanceOf[Geo].geo)
          case "password" => UTF8String.fromString(obj.asInstanceOf[Password].password)
          case "default" => UTF8String.fromString(obj.toString)
          case _ => UTF8String.fromString(obj.toString)
        }
        values(p) = objectValue
      }

    InternalRow.fromSeq(values)
  }

}

object WideNodeEncoder {

  def schema(predicates: Map[String, Predicate]): StructType =
    schema(predicates.values.toSeq)

  def schema(predicates: Seq[Predicate]): StructType =
    StructType(
      Seq(StructField("subject", LongType, nullable = false))
        ++ predicates.sortBy(_.predicateName).map(toStructField)
    )

  /**
   * Maps predicate's Dgraph types (e.g. "int" and "float") to Spark types (LongType and DoubleType, repectively)
   * @param predicate predicate
   * @return spark type
   */
  def toStructField(predicate: Predicate): StructField = {
    val dType = predicate.typeName match {
      case "uid" => LongType
      case "string" => StringType
      case "int" => LongType
      case "float" => DoubleType
      case "datetime" => TimestampType
      case "boolean" => BooleanType
      case "geo" => StringType
      case "password" => StringType
      case _ => StringType
    }
    StructField(predicate.predicateName, dType, nullable = true)
  }

}
