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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel

case class TriplePartitionReader(partition: Partition, model: GraphTableModel) extends PartitionReader[InternalRow] {

  lazy val rows: Iterator[InternalRow] = model.modelPartition(partition)

  def next: Boolean = rows.hasNext

  def get: InternalRow = rows.next()

  def close(): Unit = {}

}
