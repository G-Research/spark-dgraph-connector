package uk.co.gresearch.spark.dgraph.connector.partitioner

import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Predicate, Schema, Target}

trait ClusterStateHelper {

  def getGroupTargets(clusterState: ClusterState, group: String): Set[Target] =
    clusterState.groupMembers.getOrElse(group,
      throw new IllegalArgumentException(s"cluster state group in groupPredicates " +
        s"does not exist in groupMembers: $group"))

  def getGroupPredicates(clusterState: ClusterState, group: String, schema: Schema): Set[Predicate] =
    clusterState.groupPredicates.getOrElse(group, Set.empty).flatMap(schema.predicateMap.get)

}
