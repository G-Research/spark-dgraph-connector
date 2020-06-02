package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import uk.co.gresearch.spark.dgraph.connector.encoder.TripleEncoder

class DGraphTriplePartitionReaderFactory(encoder: TripleEncoder) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    partition match {
      case p: DGraphPartition => new DGraphTriplePartitionReader(p, encoder)
      case _ => throw new IllegalArgumentException(
        s"Expected ${DGraphPartition.getClass.getSimpleName}, not ${partition.getClass.getSimpleName}"
      )
    }

}
