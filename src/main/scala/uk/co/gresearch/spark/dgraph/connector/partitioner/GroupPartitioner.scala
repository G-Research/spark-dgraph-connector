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

package uk.co.gresearch.spark.dgraph.connector.partitioner

import uk.co.gresearch.spark.dgraph.connector.{ClusterState, GroupPartitionerOption, Partition, Schema}

case class GroupPartitioner(schema: Schema, clusterState: ClusterState)
  extends Partitioner with ClusterStateHelper {

  override def configOption: String = GroupPartitionerOption

  override def getPartitions: Seq[Partition] =
    clusterState.groupMembers.map { case (group, alphas) =>
      (group, alphas, getGroupPredicates(clusterState, group, schema))
    }.filter(_._3.nonEmpty).map { case (_, alphas, predicates) =>
      val langs = predicates.filter(_.isLang).map(_.predicateName)
      Partition(alphas.toSeq).has(predicates).langs(langs)
    }.toSeq

  override def getPartitionColumns: Option[Seq[String]] = Some(Seq("predicate"))

}
