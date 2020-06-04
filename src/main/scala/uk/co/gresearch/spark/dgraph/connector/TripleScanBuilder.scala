package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import uk.co.gresearch.spark.dgraph.connector.encoder.TripleEncoder
import uk.co.gresearch.spark.dgraph.connector.partitioner.Partitioner

class TripleScanBuilder(partitioner: Partitioner, encoder: TripleEncoder) extends ScanBuilder {
  override def build(): Scan = new TripleScan(partitioner, encoder)
}
