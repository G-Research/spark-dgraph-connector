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

import com.google.common.primitives.UnsignedLong
import uk.co.gresearch.spark.dgraph.connector.Partition

trait UidCardinalityEstimator {

  /**
   * Estimates the cardinality of uids in the given partition,
   * or None if an estimation is not available.
   *
   * @param partition a partition
   * @return estimated number of uids or None
   */
  def uidCardinality(partition: Partition): Option[UnsignedLong]

}

/**
 * A base implementation of UidCardinalityEstimator.
 */
abstract class UidCardinalityEstimatorBase extends UidCardinalityEstimator {

  /**
   * Estimates the cardinality of uids in the given partition,
   * or None if an estimation is not available.
   *
   * @param partition a partition
   * @return estimated number of uids or None
   */
  override def uidCardinality(partition: Partition): Option[UnsignedLong] =
    partition.uidRange.map(_.length).orElse(partition.uids.map(_.size.toLong)).map(UnsignedLong.valueOf)

}

case class MaxUidUidCardinalityEstimator(maxUid: Option[UnsignedLong]) extends UidCardinalityEstimatorBase {

  if (maxUid.exists(_.compareTo(UnsignedLong.ZERO) <= 0))
    throw new IllegalArgumentException(s"uidCardinality maxUid must be larger than zero: $maxUid")

  /**
   * Estimates the cardinality of uids in the given partition,
   * or None if an estimation is not available.
   *
   * @param partition a partition
   * @return estimated number of uids or None
   */
  override def uidCardinality(partition: Partition): Option[UnsignedLong] =
    super.uidCardinality(partition).orElse(maxUid)

}

object UidCardinalityEstimator {
  def forMaxUid(maxUid: Option[UnsignedLong]): UidCardinalityEstimator =
    MaxUidUidCardinalityEstimator(maxUid)
}
