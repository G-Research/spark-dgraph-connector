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

import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Partition, Schema}

case class GroupPartitioner(schema: Schema, clusterState: ClusterState)
  extends Partitioner with ClusterStateHelper {
  override def getPartitions(model: GraphTableModel): Seq[Partition] =
    clusterState.groupMembers.map { case (group, alphas) =>
      (group, alphas, getGroupPredicates(clusterState, group, schema))
    }.filter(_._3.nonEmpty).map { case (_, alphas, predicates) =>
      Partition(alphas.toSeq, predicates, None, None, model)
    }.toSeq
}
