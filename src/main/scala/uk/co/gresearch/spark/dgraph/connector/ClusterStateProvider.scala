/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
