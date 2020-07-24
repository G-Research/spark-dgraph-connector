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
import uk.co.gresearch.spark.dgraph.connector.Predicate.{columnNameForPredicateName, predicateNameForColumnName}
import uk.co.gresearch.spark.dgraph.connector.{Geo, Logging, Password, Predicate, Uid}

import scala.collection.JavaConverters._

/**
 * Encodes nodes as wide InternalRows from Dgraph json results.
 * With a projected schema the readSchema differs from the actual stored schema.
 */
case class WideNodeEncoder(predicates: Set[Predicate], projectedSchema: Option[StructType] = None)
  extends JsonNodeInternalRowEncoder
    with ProjectedSchema with ColumnInfoProvider with Logging {

  // puts subject first
  val allPredicates: Seq[Predicate] = Seq(Predicate("uid", "subject")) ++ predicates.toSeq.sortBy(_.predicateName)
  val allPredicatesMap: Map[String, Predicate] = allPredicates.map(p => p.predicateName -> p).toMap

  override def withSchema(schema: StructType): WideNodeEncoder = copy(projectedSchema = Some(schema))

  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an
   * empty schema can be returned here.
   * From: org.apache.spark.sql.connector.catalog.Table.schema
   */
  override val schema: StructType = WideNodeEncoder.schema(allPredicates)

  /**
   * Returns the predicates actually read from Dgraph. These are the predicates that correspond to readSchema
   * if projected schema is given to this encoder. Note: This preserves order of projected schema.
   */
  override val readPredicates: Option[Seq[Predicate]] =
    projectedSchema
      // ignore projected schema that is identical to schema (no projection)
      .filter(proj => proj != schema)
      // get projected predicates by name
      .map(schema => schema.fields.flatMap(col => allPredicatesMap.get(predicateNameForColumnName(col.name))))

  /**
   * Returns the actual schema of this data source scan, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   * From: org.apache.spark.sql.connector.read.Scan.readSchema
   */
  override val readSchema: StructType = readPredicates.fold(schema)(preds => WideNodeEncoder.schema(preds))

  /**
   * Column (Row) indices for all column names in read schema.
   */
  val columns: Map[String, Int] =
    readSchema.fields.zipWithIndex.map { case (p, i) =>
      (predicateNameForColumnName(p.name), i)
    }.toMap

  override val subjectColumnName: Option[String] = Some(schema.fields.head.name)
  override val predicateColumnName: Option[String] = None
  override val objectTypeColumnName: Option[String] = None
  override val objectValueColumnNames: Option[Set[String]] = Some(columns.keys.toSet)
  override val objectTypes: Option[Map[String, String]] = None

  override def isPredicateValueColumn(columnName: String): Boolean = columns.contains(columnName)

  /**
   * Encodes the given Dgraph json result into InternalRows.
   *
   * @param result Json result
   * @return internal rows
   */
  override def fromJson(result: JsonArray): Iterator[InternalRow] = getNodes(result).map(toNode)

  /**
   * Encodes a node as a wide InternalRow.
   *
   * @param node a json node to turn into a wide InternalRow
   * @return InternalRows
   */
  def toNode(node: JsonObject): InternalRow = {
    val values = Array.fill[Any](columns.size)(null)

    try {
      // put all values into corresponding columns of 'values'
      node
        .entrySet()
        .iterator().asScala
        .map { e =>
          (
            columns.get(e.getKey),
            allPredicatesMap.get(e.getKey).map(_.dgraphType),
            e.getValue,
          )
        }
        .filter(e => e._1.isDefined && e._2.isDefined)
        .foreach {
          case (Some(p), Some(t), o) =>
            val obj = getValue(o, t)
            val objectValue = t match {
              case "subject" => obj.asInstanceOf[Uid].uid
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
          case (_, _, _) =>
            throw new IllegalStateException("should never happen, " +
              "the filter before foreach guarantees that, " +
              "this line is just to avoid compile warning")
        }

      InternalRow.fromSeq(values)
    } catch {
      case t: Throwable =>
        log.error(s"failed to encode node: $node")
        throw t
    }
  }

}

object WideNodeEncoder {

  def schema(predicates: Map[String, Predicate]): StructType =
    schema(predicates.values.toSeq)

  def schema(predicates: Seq[Predicate]): StructType =
    StructType(
      // exclude edges
      predicates.filterNot(_.isEdge).map(toStructField)
    )

  /**
   * Maps predicate's Dgraph types (e.g. "int" and "float") to Spark types (LongType and DoubleType, respectively)
   *
   * @param predicate predicate
   * @return spark type
   */
  def toStructField(predicate: Predicate): StructField = {
    val dType = predicate.dgraphType match {
      case "subject" => LongType
      case "string" => StringType
      case "int" => LongType
      case "float" => DoubleType
      case "datetime" => TimestampType
      case "boolean" => BooleanType
      case "geo" => StringType
      case "password" => StringType
      case _ => StringType
    }
    StructField(columnNameForPredicateName(predicate.predicateName), dType, nullable = predicate.dgraphType != "subject")
  }

}
