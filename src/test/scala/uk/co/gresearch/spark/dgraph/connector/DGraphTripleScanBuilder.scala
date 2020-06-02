package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import uk.co.gresearch.spark.dgraph.connector.encoder.TripleEncoder

class DGraphTripleScanBuilder(targets: Seq[Target], encoder: TripleEncoder) extends ScanBuilder {
  override def build(): Scan = new DGraphTripleScan(targets, encoder)
}
