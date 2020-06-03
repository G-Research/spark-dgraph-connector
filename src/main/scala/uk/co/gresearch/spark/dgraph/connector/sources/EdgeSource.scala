package uk.co.gresearch.spark.dgraph.connector.sources

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.encoder.{EdgeEncoder, StringObjectTripleEncoder, TypedObjectTripleEncoder}
import uk.co.gresearch.spark.dgraph.connector.{DGraphTripleTable, SchemaProvider, TableProviderBase, Target, TriplesModeOption, TriplesModeStringObjectsOption, TriplesModeTypedObjectsOption}

class EdgeSource() extends TableProviderBase with SchemaProvider {

  override def shortName(): String = "dgraph-edges"

  def getTable(options: CaseInsensitiveStringMap): Table = {
    val targets = getTargets(options)
    val schema = getSchema(targets).filter(_._2 == "uid")
    val encoder = new EdgeEncoder()
    new DGraphTripleTable(targets, schema, encoder)
  }

}
