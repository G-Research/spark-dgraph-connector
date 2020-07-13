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

  override def incReadBytes(bytes: Long): Unit = {
    println(s"increment accum $readBytes")
    readBytes.add(bytes)
  }

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
