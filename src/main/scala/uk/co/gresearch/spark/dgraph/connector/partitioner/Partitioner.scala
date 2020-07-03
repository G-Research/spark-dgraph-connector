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

import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.Partition

trait Partitioner {

  /**
   * Gets the partitions.
   */
  def getPartitions: Seq[Partition]

  /**
   * Indicates whether this partitioner supports the given filter.
   * @param filter filter
   * @return true if supported, false otherwise
   */
  def supportsFilter(filter: connector.Filter): Boolean = false

  /**
   * Sets the filters to be used by the partitioner. Returns a copy of this partitioner with the filters set.
   * Partitioner tries its best to utilize filters. In the worst case, no filter is being considered by partitioning.
   *
   * @param filters filters
   * @return partitioner with the given filters
   */
  def withFilters(filters: Seq[connector.Filter]): Partitioner = this

}
