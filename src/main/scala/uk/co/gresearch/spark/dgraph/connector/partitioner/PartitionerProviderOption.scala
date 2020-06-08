package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Schema, Target}

trait PartitionerProviderOption {

  def getPartitioner(schema: Schema,
                     clusterState: ClusterState,
                     options: CaseInsensitiveStringMap): Option[Partitioner]

  protected def allClusterTargets(clusterState: ClusterState): Seq[Target] =
    clusterState.groupMembers.values.flatten.toSeq

}
