package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class DGraphTriplePartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    partition match {
      case p: DGraphPartition => new DGraphTriplePartitionReader(p)
      case _ => throw new IllegalArgumentException(
        s"Expected ${DGraphPartition.getClass.getSimpleName}, not ${partition.getClass.getSimpleName}"
      )
    }

}
