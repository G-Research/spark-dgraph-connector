package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap


class DGraphTripleTableProvider() extends TableProviderBase {

  override def shortName(): String = "dgraph-triples"

  def getTable(options: CaseInsensitiveStringMap): Table = {
    val targets: Seq[Target] = getTargets(options)
    new DGraphTripleTable(targets)
  }

}
