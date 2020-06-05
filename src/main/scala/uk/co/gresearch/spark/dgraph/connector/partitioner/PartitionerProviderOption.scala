package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Schema, Target}

trait PartitionerProviderOption {

  def getPartitioner(targets: Seq[Target],
                     schema: Schema,
                     clusterState: ClusterState,
                     options: CaseInsensitiveStringMap): Option[Partitioner]

}
