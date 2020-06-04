package uk.co.gresearch.spark.dgraph.connector.sources

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.encoder.{EdgeEncoder, TypedNodeEncoder}
import uk.co.gresearch.spark.dgraph.connector.{TripleTable, SchemaProvider, TableProviderBase}

class NodeSource() extends TableProviderBase with SchemaProvider {

  override def shortName(): String = "dgraph-nodes"

  def getTable(options: CaseInsensitiveStringMap): Table = {
    val targets = getTargets(options)
    val schema = getSchema(targets).filter(_.typeName != "uid")
    val encoder = new TypedNodeEncoder()
    new TripleTable(targets, schema, encoder)
  }

}
