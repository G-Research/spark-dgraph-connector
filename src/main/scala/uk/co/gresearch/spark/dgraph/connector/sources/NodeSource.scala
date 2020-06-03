package uk.co.gresearch.spark.dgraph.connector.sources

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.encoder.{EdgeEncoder, NodeEncoder}
import uk.co.gresearch.spark.dgraph.connector.{DGraphTripleTable, SchemaProvider, TableProviderBase}

class NodeSource() extends TableProviderBase with SchemaProvider {

  override def shortName(): String = "dgraph-nodes"

  def getTable(options: CaseInsensitiveStringMap): Table = {
    val targets = getTargets(options)
    val schema = getSchema(targets).filter(_._2 != "uid")
    val encoder = new NodeEncoder()
    new DGraphTripleTable(targets, schema, encoder)
  }

}
