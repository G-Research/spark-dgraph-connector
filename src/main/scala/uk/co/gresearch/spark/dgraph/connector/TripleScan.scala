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

import org.apache.spark.sql.connector.expressions.{Expression, NamedReference, Transform}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.TripleScan.namedReference
import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel
import uk.co.gresearch.spark.dgraph.connector.partitioner.Partitioner

case class TripleScan(partitioner: Partitioner, model: GraphTableModel)
  extends Scan with SupportsReportPartitioning
    with Batch {

  override def readSchema(): StructType = model.readSchema()

  override def toBatch: Batch = this

  private lazy val partitions: Array[InputPartition] = partitioner.getPartitions.toArray

  private lazy val partitionKeys: Option[Array[Expression]] =
    partitioner.getPartitionColumns.map(partitionColumns =>
      partitionColumns.map(partitionColumn => namedReference(partitionColumn)).toArray
    )

  override def planInputPartitions(): Array[InputPartition] = partitions

  override def createReaderFactory(): PartitionReaderFactory =
    TriplePartitionReaderFactory(model.withMetrics(AccumulatorPartitionMetrics()))

  override def outputPartitioning(): Partitioning =
    partitionKeys.map(keys => new KeyGroupedPartitioning(keys, partitions.length))
      .getOrElse(new UnknownPartitioning(partitions.length))

}

object TripleScan {
  def namedReference(columnName: String): Expression =
    new Transform {
      override def name(): String = "identity"

      override def references(): Array[NamedReference] = Array.empty

      override def arguments(): Array[Expression] = Array(new NamedReference {
        override def fieldNames(): Array[String] = Array(columnName)
      })
    }
}
