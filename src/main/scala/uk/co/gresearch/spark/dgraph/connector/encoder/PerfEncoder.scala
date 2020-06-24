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

import collection.JavaConverters._
import com.google.gson.{Gson, JsonArray}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import uk.co.gresearch.spark.dgraph.connector.{Json, Perf, PerfJson}

/**
 * Encodes a perf json response.
 */
case class PerfEncoder() extends JsonNodeInternalRowEncoder {

  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an
   * empty schema can be returned here.
   * From: org.apache.spark.sql.connector.catalog.Table.schema
   */
  override def schema(): StructType = Encoders.product[Perf].schema

  /**
   * Returns the actual schema of this data source scan, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   * From: org.apache.spark.sql.connector.read.Scan.readSchema
   */
  override def readSchema(): StructType = schema()

  /**
   * Encodes the given Dgraph json result into InternalRows.
   *
   * @param result Json result
   * @return internal rows
   */
  override def fromJson(result: JsonArray): Iterator[InternalRow] = {
    // look into first element, it will have extra perf payload
    // drop the array and only encode that payload
    result.iterator().asScala
      .map(_.asInstanceOf[PerfJson])
      .map(perf =>
        InternalRow(
          new GenericArrayData(perf.partitionTargets.map(UTF8String.fromString)),
          Option(perf.partitionPredicates).map(p => new GenericArrayData(p.map(UTF8String.fromString))).orNull,
          perf.partitionUidsFirst,
          perf.partitionUidsLength,

          perf.sparkStageId,
          perf.sparkStageAttemptNumber,
          perf.sparkPartitionId,
          perf.sparkAttemptNumber,
          perf.sparkTaskAttemptId,

          perf.dgraphAssignTimestamp,
          perf.dgraphParsing,
          perf.dgraphProcessing,
          perf.dgraphEncoding,
          perf.dgraphTotal
        )
      )
  }

  /**
   * Indicates whether the column name refers to the subject column.
   * Must be implemented to support filter pushdown.
   *
   * @param columnName column name
   * @return true if column is the subject
   */
  override def isSubjectColumn(columnName: String): Boolean = ???

  /**
   * Indicates whether the column name refers to the predicate column.
   * Must be implemented to support filter pushdown.
   *
   * @param columnName column name
   * @return true if column is the predicate
   */
override def isPredicateColumn(columnName: String): Boolean = ???

  /**
   * Indicates whether the column name refers to a specific predicate and its (object) value.
   * Must be implemented to support filter pushdown.
   *
   * @param columnName column name
   * @return true if column is a predicate's value
   */
  override def isPredicateValueColumn(columnName: String): Boolean = ???

  /**
   * Indicates whether the column name refers to the object type.
   * Must be implemented to support filter pushdown.
   *
   * @param columnName column name
   * @return true if column is the object type
   */
  override def isObjectTypeColumn(columnName: String): Boolean = ???

  /**
   * Indicates whether the column name refers to the object value.
   * Must be implemented to support filter pushdown.
   *
   * @param columnName column name
   * @return true if column is the object value
   */
  override def isObjectValueColumn(columnName: String): Boolean = ???

  /**
   * Provides the type of the given object value column.
   * Must be implemented to support filter pushdown.
   *
   * @param columnName column name
   * @return object type
   */
  override def getObjectType(columnName: String): Option[String] = ???
}
