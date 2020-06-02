package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.encoder.TripleEncoder

class DGraphTripleScan(targets: Seq[Target], encoder: TripleEncoder) extends Scan with Batch {

  override def readSchema(): StructType = encoder.readSchema()

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(DGraphPartition(targets))
  }

  override def createReaderFactory(): PartitionReaderFactory = new DGraphTriplePartitionReaderFactory(encoder)

}
