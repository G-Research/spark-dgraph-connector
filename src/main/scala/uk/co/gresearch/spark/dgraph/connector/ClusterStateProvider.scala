package uk.co.gresearch.spark.dgraph.connector

import java.net.ConnectException

trait ClusterStateProvider {

  def getClusterState(targets: Seq[Target]): ClusterState = {
    val clusterStates = targets.map(getClusterState).flatten
    val cids = clusterStates.map(_.cid).toSet
    if (cids.size > 1)
      throw new RuntimeException(s"Retrived multiple cluster ids from " +
        s"Dgraph alphas (${targets.map(_.target).mkString(", ")}): ${cids.mkString(", ")}")
    clusterStates.headOption.getOrElse(
      throw new RuntimeException(s"Could not retrieve cluster state from Dgraph alphas (${targets.map(_.target).mkString(", ")})")
    )
  }

  def getClusterState(target: Target): Option[ClusterState] = {
    val url = s"http://${target.withPort(target.port-1000).target}/state"
    try {
      val request = requests.get(url)
      if (request.statusCode == 200) {
        Some(ClusterState.fromJson(request.text()))
      } else {
        println(s"retrieving state from $url failed: ${request.statusCode} ${request.statusMessage}")
        None
      }
    } catch {
      case _: Throwable =>
        println(s"retrieving state from $url failed")
        None
    }
  }

}
