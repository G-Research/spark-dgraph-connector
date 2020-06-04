package uk.co.gresearch.spark.dgraph.connector

import java.util.UUID

trait ClusterStateProvider {

  def getClusterState(targets: Seq[Target]): ClusterState =
    ClusterState(Map.empty[String, Set[Target]], Map.empty[String, Set[String]], 0, UUID.randomUUID())

}
