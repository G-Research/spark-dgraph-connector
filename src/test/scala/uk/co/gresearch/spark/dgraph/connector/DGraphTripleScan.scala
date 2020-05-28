package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class DGraphPartition(targets: Seq[Target]) extends InputPartition {
  // TODO: use host names of DGraph alphas to co-locate partitions
  override def preferredLocations(): Array[String] = super.preferredLocations()
}

class DGraphTripleScan(targets: Seq[Target]) extends Scan with Batch {

  override def readSchema(): StructType = StructType(
    Array(
      StructField("s", StringType),
      StructField("p", StringType),
      StructField("o", StringType)
    )
  )

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(DGraphPartition(targets))
  }

  override def createReaderFactory(): PartitionReaderFactory = new DGraphTriplePartitionReaderFactory

}
