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

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel
import uk.co.gresearch.spark.dgraph.connector.partitioner.Partitioner

case class TripleScan(partitioner: Partitioner, model: GraphTableModel)
  extends Scan
    with Batch {

  override def readSchema(): StructType = model.readSchema()

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = partitioner.getPartitions.toArray

  override def createReaderFactory(): PartitionReaderFactory =
    TriplePartitionReaderFactory(model.withMetrics(AccumulatorPartitionMetrics()))

}
