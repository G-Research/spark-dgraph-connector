package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import uk.co.gresearch.spark.dgraph.connector.encoder.TripleEncoder

class TriplePartitionReader(partition: Partition, encoder: TripleEncoder) extends PartitionReader[InternalRow] {

  lazy val triples: Iterator[Triple] = partition.getTriples

  def next: Boolean = triples.hasNext

  def get: InternalRow = encoder.asInternalRow(triples.next())

  def close(): Unit = Unit

}
