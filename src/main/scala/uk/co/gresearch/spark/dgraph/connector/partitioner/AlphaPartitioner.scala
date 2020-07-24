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

import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Partition, Schema}

case class AlphaPartitioner(schema: Schema, clusterState: ClusterState, partitionsPerAlpha: Int)
  extends Partitioner with ClusterStateHelper {

  if (partitionsPerAlpha <= 0)
    throw new IllegalArgumentException(s"partitionsPerAlpha must be larger than zero: $partitionsPerAlpha")

  override def getPartitions(implicit model: GraphTableModel): Seq[Partition] = {
    PredicatePartitioner.getPartitions(
      schema, clusterState, getGroupTargets(clusterState, _).size * partitionsPerAlpha, Set.empty, None, model
    )
  }

}
