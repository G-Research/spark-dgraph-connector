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

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import uk.co.gresearch.spark.dgraph.connector.{StringTriple, Triple, TriplesFactory}

/**
 * Encodes Triple by representing objects as strings.
 **/
case class StringTripleEncoder(triplesFactory: TriplesFactory) extends TripleEncoder {

  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an
   * empty schema can be returned here.
   * From: org.apache.spark.sql.connector.catalog.Table.schema
   */
  override def schema(): StructType = Encoders.product[StringTriple].schema

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
  override def asInternalRow(triple: Triple): InternalRow =
    InternalRow(
      triple.s.uid,
      UTF8String.fromString(triple.p),
      UTF8String.fromString(triple.o.toString),
      UTF8String.fromString(triplesFactory.getType(triple.o)),
    )

}
