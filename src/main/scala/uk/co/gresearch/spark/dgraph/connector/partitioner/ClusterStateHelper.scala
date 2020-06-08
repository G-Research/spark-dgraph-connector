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
