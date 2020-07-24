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

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}

trait PartitionMetrics {

  def incReadBytes(bytes: Long): Unit

  def incReadUids(uids: Long): Unit

  def incReadChunks(chunks: Long): Unit

  def incChunkTime(time: Double): Unit

}

case class AccumulatorPartitionMetrics(readBytes: LongAccumulator,
                                       readUids: LongAccumulator,
                                       readChunks: LongAccumulator,
                                       chunkTime: DoubleAccumulator)
  extends PartitionMetrics with Serializable {

  override def incReadBytes(bytes: Long): Unit = readBytes.add(bytes)

  override def incReadUids(uids: Long): Unit = readUids.add(uids)

  override def incReadChunks(chunks: Long): Unit = readChunks.add(chunks)

  override def incChunkTime(time: Double): Unit = chunkTime.add(time)

}

object AccumulatorPartitionMetrics {
  def apply(): AccumulatorPartitionMetrics = AccumulatorPartitionMetrics(SparkSession.builder().getOrCreate())

  def apply(spark: SparkSession): AccumulatorPartitionMetrics = AccumulatorPartitionMetrics(spark.sparkContext)

  def apply(context: SparkContext): AccumulatorPartitionMetrics =
    AccumulatorPartitionMetrics(
      context.longAccumulator("Dgraph Bytes"),
      context.longAccumulator("Dgraph Uids"),
      context.longAccumulator("Dgraph Chunks"),
      context.doubleAccumulator("Dgraph Time")
    )
}

case class NoPartitionMetrics() extends PartitionMetrics with Serializable {
  override def incReadBytes(bytes: Long): Unit = { }

  override def incReadUids(uids: Long): Unit = { }

  override def incReadChunks(chunks: Long): Unit = { }

  override def incChunkTime(time: Double): Unit = { }
}
