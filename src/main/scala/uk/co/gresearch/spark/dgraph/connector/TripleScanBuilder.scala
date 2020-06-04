package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import uk.co.gresearch.spark.dgraph.connector.encoder.TripleEncoder

class TripleScanBuilder(targets: Seq[Target], schema: Schema, encoder: TripleEncoder) extends ScanBuilder {
  override def build(): Scan = new TripleScan(targets, schema, encoder)
}
