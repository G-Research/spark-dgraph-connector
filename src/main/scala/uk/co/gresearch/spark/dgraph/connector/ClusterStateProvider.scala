/*
 * Copyright 2020 G-Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector

import java.time.Clock

trait ClusterStateProvider extends Logging {

  def getClusterState(target: Target): Option[ClusterState] = {
    val url = s"http://${target.withPort(target.port-1000).target}/state"
    try {
      val startTs = Clock.systemUTC().instant().toEpochMilli
      val request = requests.get(url)
      val endTs = Clock.systemUTC().instant().toEpochMilli
      val json = Json(request.text())

      log.info(s"retrieved cluster state from ${target.target} " +
        s"with ${json.string.getBytes.length} bytes " +
        s"in ${(endTs - startTs) / 1000.0}s")
      log.trace(s"retrieved cluster state: ${abbreviate(json.string)}")

      if (request.statusCode == 200) {
        try {
          Some(ClusterState.fromJson(json))
        } catch {
          case t: Throwable =>
            log.error(s"failed to parse cluster state json: ${abbreviate(json.string)}")
            throw t
        }
      } else {
        log.error(s"retrieving state from $url failed: ${request.statusCode} ${request.statusMessage}")
        None
      }
    } catch {
      case t: Throwable =>
        log.error(s"retrieving state from $url failed: ${t.getMessage}")
        None
    }
  }

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

}
