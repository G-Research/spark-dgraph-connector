package uk.co.gresearch.spark.dgraph.connector.partitioner

import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Partition, Schema}

case class GroupPartitioner(schema: Schema, clusterState: ClusterState)
  extends Partitioner with ClusterStateHelper {
  override def getPartitions: Seq[Partition] =
    clusterState.groupMembers.map { case (group, alphas) =>
      (group, alphas, getGroupPredicates(clusterState, group, schema))
    }.filter(_._3.nonEmpty).map { case (_, alphas, predicates) =>
      Partition(alphas.toSeq, Some(predicates), None)
    }.toSeq
}
