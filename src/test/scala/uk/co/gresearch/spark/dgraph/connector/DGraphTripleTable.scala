package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DGraphTripleTable(val targets: Seq[Target]) extends DGraphTableBase {

  override def schema(): StructType = StructType(
    Array(
      StructField("s", StringType),
      StructField("p", StringType),
      StructField("o", StringType)
    )
  )

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new DGraphTripleScanBuilder(targets)

}
