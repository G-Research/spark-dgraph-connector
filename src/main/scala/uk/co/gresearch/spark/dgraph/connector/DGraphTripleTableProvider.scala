package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.encoder.{StringObjectTripleEncoder, TypedObjectTripleEncoder}


class DGraphTripleTableProvider() extends TableProviderBase {

  override def shortName(): String = "dgraph-triples"

  def getTripleMode(map: CaseInsensitiveStringMap): Option[String] =
    Option(map.get(TriplesModeOption))

  def getTable(options: CaseInsensitiveStringMap): Table = {
    val targets: Seq[Target] = getTargets(options)
    val tripleMode: Option[String] = getTripleMode(options)
    val encoder = tripleMode match {
      case Some(TriplesModeStringObjectsOption) => new StringObjectTripleEncoder
      case Some(TriplesModeTypedObjectsOption) => new TypedObjectTripleEncoder
      case Some(mode) => throw new IllegalArgumentException(s"Unknown triple mode: ${mode}")
      case None => new StringObjectTripleEncoder
    }
    new DGraphTripleTable(targets, encoder)
  }

}
