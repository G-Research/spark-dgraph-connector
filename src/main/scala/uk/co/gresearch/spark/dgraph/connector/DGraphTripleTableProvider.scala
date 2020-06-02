package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.encoder.StringObjectTripleEncoder


class DGraphTripleTableProvider() extends TableProviderBase {

  override def shortName(): String = "dgraph-triples"

  def getTable(options: CaseInsensitiveStringMap): Table = {
    val targets: Seq[Target] = getTargets(options)
    val encoder = new StringObjectTripleEncoder
    new DGraphTripleTable(targets, encoder)
  }

}
