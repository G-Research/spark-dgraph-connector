package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}

class DGraphTripleScanBuilder(targets: Seq[Target]) extends ScanBuilder {
  override def build(): Scan = new DGraphTripleScan(targets)
}
