package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import uk.co.gresearch.spark.dgraph.connector.encoder.{EdgeEncoder, TripleEncoder, TypedNodeEncoder}

class TriplePartitionReader(partition: Partition, encoder: TripleEncoder) extends PartitionReader[InternalRow] {

  lazy val triples: Iterator[Triple] =
    encoder match {
      case _: EdgeEncoder => partition.getEdgeTriples
      case _: TypedNodeEncoder => partition.getNodeTriples
      case _ => partition.getTriples
    }

  def next: Boolean = triples.hasNext

  def get: InternalRow = encoder.asInternalRow(triples.next())

  def close(): Unit = Unit

}
