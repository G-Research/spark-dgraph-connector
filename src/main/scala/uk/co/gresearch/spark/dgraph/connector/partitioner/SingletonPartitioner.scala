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

import uk.co.gresearch.spark.dgraph.connector.{ClusterState, EmptyFilters, Filters, Predicate, Schema, SingletonPartitionerOption}

/**
 * This partitioner produces a single partition, but provides the features
 * of the PredicatePartitioner (filtering and projection, though for a single partition).
 */
class SingletonPartitioner(override val schema: Schema,
                           override val clusterState: ClusterState,
                           override val filters: Filters = EmptyFilters,
                           override val projection: Option[Seq[Predicate]] = None)
  extends PredicatePartitioner(schema, clusterState, Int.MaxValue, filters, projection) {

  override def configOption: String = SingletonPartitionerOption

  override def getPartitionColumns: Option[Seq[String]] = None

  override def withFilters(filters: Filters): Partitioner =
    new SingletonPartitioner(schema, clusterState, filters, projection)

  override def withProjection(projection: Seq[Predicate]): Partitioner =
    new SingletonPartitioner(schema, clusterState, filters, Some(projection))

}

object SingletonPartitioner {
  def apply(schema: Schema, clusterState: ClusterState): SingletonPartitioner =
    new SingletonPartitioner(schema, clusterState)
}
