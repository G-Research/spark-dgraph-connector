package uk.co.gresearch.spark.dgraph.connector.sources

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.encoder.{StringTripleEncoder, TypedTripleEncoder}
import uk.co.gresearch.spark.dgraph.connector.partitioner.PartitionerProvider
import uk.co.gresearch.spark.dgraph.connector.{ClusterStateProvider, SchemaProvider, TableProviderBase, TargetsConfigParser, TripleTable, TriplesModeOption, TriplesModeStringOption, TriplesModeTypedOption}

class TripleSource() extends TableProviderBase
  with TargetsConfigParser with SchemaProvider
  with ClusterStateProvider with PartitionerProvider {

  override def shortName(): String = "dgraph-triples"

  def getTripleMode(options: CaseInsensitiveStringMap): Option[String] =
    getStringOption(TriplesModeOption, options)

  def getTable(options: CaseInsensitiveStringMap): Table = {
    val targets = getTargets(options)
    val schema = getSchema(targets)
    val clusterState = getClusterState(targets)
    val partitioner = getPartitioner(targets, schema, clusterState, options)
    val tripleMode = getTripleMode(options)
    val encoder = tripleMode match {
      case Some(TriplesModeStringOption) => new StringTripleEncoder
      case Some(TriplesModeTypedOption) => new TypedTripleEncoder
      case Some(mode) => throw new IllegalArgumentException(s"Unknown triple mode: ${mode}")
      case None => new TypedTripleEncoder
    }
    new TripleTable(partitioner, encoder, clusterState.cid)
  }

}
