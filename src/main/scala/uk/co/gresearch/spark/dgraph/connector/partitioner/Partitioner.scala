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

import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.{Filters, Partition, Predicate}

trait Partitioner {

  /**
   * The option string used for config "dgraph.partitioner" (PartitionerOption) to reference this partitioner.
   * @return config option string
   */
  def configOption: String

  /**
   * Gets the partitions.
   */
  def getPartitions: Seq[Partition]

  /**
   * Get the column names that represent the partitioning.
   * @return
   */
  def getPartitionColumns: Option[Seq[String]]

  /**
   * Indicates whether this partitioner supports all given filters.
   * @param filters filters
   * @return true if supported, false otherwise
   */
  def supportsFilters(filters: Set[connector.Filter]): Boolean = false

  /**
   * Sets the filters to be used by the partitioner. Returns a copy of this partitioner with the filters set.
   * Partitioner has to use the required filters and can use the optional filters.
   * Actual given filters have to comply with result of supportsFilters.
   * Default implementation ignores given filters.
   *
   * @param filters filters
   * @return partitioner with the given filters
   */
  def withFilters(filters: Filters): Partitioner = this

  /**
   * Sets the projection to be used by the partitioner. Returns a copy of this partitioner with the projection.
   * Partitioner can use the projection if supported.
   * Default implementation ignores projection.
   *
   * @param projection projection
   * @return partitioner with the given projection
   */
  def withProjection(projection: Seq[Predicate]): Partitioner = this

}
