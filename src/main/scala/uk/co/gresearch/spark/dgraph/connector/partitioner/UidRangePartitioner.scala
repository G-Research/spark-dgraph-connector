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
import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.{Filter, Filters, Logging, Partition, Uid, UidRange}

case class UidRangePartitioner(partitioner: Partitioner,
                               uidsPerPartition: Int,
                               maxPartitions: Int,
                               uidCardinalityEstimator: UidCardinalityEstimator) extends Partitioner with Logging {

  if (partitioner == null)
    throw new IllegalArgumentException("partitioner must not be null")

  if (uidsPerPartition <= 0)
    throw new IllegalArgumentException(s"uidsPerPartition must be larger than zero: $uidsPerPartition")

  if (maxPartitions <= 0)
    throw new IllegalArgumentException(s"maxPartitions must be larger than zero: $maxPartitions")

  val uidsPerPartitionUnsigned: UnsignedLong = UnsignedLong.valueOf(uidsPerPartition)
  val maxPartitionsUnsigned: UnsignedLong = UnsignedLong.valueOf(maxPartitions)

  val partitions: Seq[Partition] = partitioner.getPartitions

  if (partitions.exists(_.uidRange.isDefined))
    throw new IllegalArgumentException(s"UidRangePartitioner cannot be combined with " +
      s"another uid partitioner: ${partitioner.getClass.getSimpleName}")

  override def supportsFilters(filters: Set[Filter]): Boolean = partitioner.supportsFilters(filters)

  override def withFilters(filters: Filters): UidRangePartitioner = copy(partitioner = partitioner.withFilters(filters))

  override def withProjection(projection: Seq[connector.Predicate]): UidRangePartitioner = copy(partitioner = partitioner.withProjection(projection))

  override def getPartitions: Seq[Partition] = {
    partitions.flatMap { partition =>
      val uidCardinality = uidCardinalityEstimator.uidCardinality(partition)
      val somePartitions = uidCardinality.map(uids => uids.minus(UnsignedLong.ONE).dividedBy(uidsPerPartitionUnsigned).plus(UnsignedLong.ONE))

      somePartitions
        .filter(_.compareTo(UnsignedLong.ONE) > 0)
        .map { parts =>
          if (parts.compareTo(maxPartitionsUnsigned) <= 0) {
            (0 to parts.intValue())
              .map(idx => 1 + idx * uidsPerPartition)
              .map(Uid(_))
              .sliding(2)
              .map(uids => UidRange(uids.head, uids.last))
              .zipWithIndex
              .map {
                case (range, idx) =>
                  Partition(partition.targets.rotateLeft(idx), partition.operators + range)
              }
          } else {
            if (parts.compareTo(UnsignedLong.ONE) > 0) {
              log.warn(s"Will not partition by uid range as this leads to more then $maxPartitions partitions: ${parts} " +
                s"(uidsPerPartition=$uidsPerPartition, uidCardinality=${uidCardinality.get})")
            }
            Seq(partition)
          }
        }.getOrElse(Seq(partition))
    }
  }

}
