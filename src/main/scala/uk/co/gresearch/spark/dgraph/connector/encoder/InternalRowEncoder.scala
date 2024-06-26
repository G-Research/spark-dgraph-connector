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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.Json

/**
 * Encodes data into InternalRows. Supports Dgraph json results as input.
 */
trait InternalRowEncoder extends ColumnInfo {

  /**
   * Sets the schema of this encoder. This encoder may only partially or not at all use the given schema. Default
   * implementation ignores the given schema completely.
   * @param schema
   *   a schema
   * @return
   *   encoder with the given schema
   */
  def withSchema(schema: StructType): InternalRowEncoder = this

  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an empty schema can be
   * returned here. From: org.apache.spark.sql.connector.catalog.Table.schema
   */
  def schema(): StructType

  /**
   * Returns the actual schema of this data source scan, which may be different from the physical schema of the
   * underlying storage, as column pruning or other optimizations may happen. From:
   * org.apache.spark.sql.connector.read.Scan.readSchema
   */
  def readSchema(): StructType

  /**
   * Encodes the given Dgraph json result into InternalRows.
   *
   * @param json
   *   Json result
   * @param member
   *   member in the json that has the result
   * @return
   *   internal rows
   */
  def fromJson(json: Json, member: String): Iterator[InternalRow]

}
