package uk.co.gresearch.spark.dgraph.connector.sources

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.encoder.{EdgeEncoder, StringTripleEncoder, TypedTripleEncoder}
import uk.co.gresearch.spark.dgraph.connector.{TripleTable, SchemaProvider, TableProviderBase, Target, TriplesModeOption, TriplesModeStringOption, TriplesModeTypedOption}

class EdgeSource() extends TableProviderBase with SchemaProvider {

  override def shortName(): String = "dgraph-edges"

  def getTable(options: CaseInsensitiveStringMap): Table = {
    val targets = getTargets(options)
    val schema = getSchema(targets).filter(_._2 == "uid")
    val encoder = new EdgeEncoder()
    new TripleTable(targets, schema, encoder)
  }

}
