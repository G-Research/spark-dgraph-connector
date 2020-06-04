package uk.co.gresearch.spark.dgraph.connector

import java.util.UUID

import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.encoder.TripleEncoder
import uk.co.gresearch.spark.dgraph.connector.partitioner.Partitioner

class TripleTable(partitioner: Partitioner, encoder: TripleEncoder, val cid: UUID) extends TableBase {

  override def schema(): StructType = encoder.schema()

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new TripleScanBuilder(partitioner, encoder)

}
