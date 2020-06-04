package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.encoder.TripleEncoder

class TripleTable(val targets: Seq[Target], schema: Schema, encoder: TripleEncoder) extends TableBase {

  override def schema(): StructType = encoder.schema()

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new TripleScanBuilder(targets, schema, encoder)

}
