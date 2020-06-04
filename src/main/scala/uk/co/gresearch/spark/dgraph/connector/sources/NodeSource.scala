package uk.co.gresearch.spark.dgraph.connector.sources

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.encoder.TypedNodeEncoder
import uk.co.gresearch.spark.dgraph.connector.{ClusterStateProvider, PartitionerProvider, SchemaProvider, TableProviderBase, TargetsConfigParser, TripleTable}

class NodeSource() extends TableProviderBase
  with TargetsConfigParser with SchemaProvider
  with ClusterStateProvider with PartitionerProvider {

  override def shortName(): String = "dgraph-nodes"

  def getTable(options: CaseInsensitiveStringMap): Table = {
    val targets = getTargets(options)
    val schema = getSchema(targets).filter(_.typeName != "uid")
    val clusterState = getClusterState(targets)
    val partitioner = getPartitioner(targets, schema, clusterState, options)
    val encoder = new TypedNodeEncoder()
    new TripleTable(partitioner, encoder, clusterState.cid)
  }

}
