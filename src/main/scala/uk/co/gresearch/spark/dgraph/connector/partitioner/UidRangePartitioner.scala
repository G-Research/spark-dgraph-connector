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
import uk.co.gresearch.spark.dgraph.connector.{Filter, Filters, Logging, Partition, Uid, UidRange, UidRangePartitionerMaxPartsOption, UidRangePartitionerUidsPerPartOption}

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
      // when no uid cardinality is available we consider the entire uid value range
      // we handle that case anyway so this is a good fallback
      val uidCardinality = uidCardinalityEstimator.uidCardinality(partition).getOrElse(UnsignedLong.MAX_VALUE)

      // compute number of equal-sized partitions that cover uidCardinality
      val numPartitions = divideAndCeil(uidCardinality, uidsPerPartitionUnsigned)
      if (numPartitions.compareTo(UnsignedLong.ONE) <= 0) {
        // single partition suffices (input partition)
        Seq(partition)
      } else if (numPartitions.compareTo(maxPartitionsUnsigned) <= 0) {
        // number of partitions is allowed
        createUniformPartitions(partition, numPartitions.intValue(), uidsPerPartition, UnsignedLong.ONE)
      } else {
        // uniform partitions over uid cardinality would create too many partitions
        //
        // one reason for too many ids (except from too low values for uidsPerPartition and maxPartitions)
        // is data has been loaded via live loader, which allocates a dense section of uids at the beginning
        // of the uids range (here called dense), and randomly distributed uids in the remaining part of the
        // uid range (here called sparse)
        val (densePartitions, sparsePartitions, denseUids) = getDenseAndSparsePartitions(uidCardinality)
        createUniformPartitions(partition, densePartitions.intValue(), divideAndCeil(denseUids, densePartitions).intValue(), UnsignedLong.ONE) ++
          createUniformPartitions(partition, sparsePartitions.intValue(), divideAndCeil(uidCardinality.minus(denseUids), sparsePartitions).intValue(), denseUids)
      }
    }
  }

  private def getDenseAndSparseUids(uids: UnsignedLong): (UnsignedLong, UnsignedLong) =
    (uids, UnsignedLong.ZERO)

  private[connector] def divideAndCeil(devisor: UnsignedLong, denominator: UnsignedLong): UnsignedLong = {
    if (devisor.compareTo(UnsignedLong.ZERO) == 0) {
      devisor
    } else {
      devisor.minus(UnsignedLong.ONE).dividedBy(denominator).plus(UnsignedLong.ONE)
    }
  }

  private[connector] def createUniformPartitions(partition: Partition, partitions: Int, uidsPerPartition: Int, first: UnsignedLong): Iterator[Partition] = {
    if (partitions == 0) {
      Iterator.empty
    } else {
      (0 to partitions.intValue())
        .map(idx => first.plus(UnsignedLong.valueOf(idx * uidsPerPartition)))
        .map(Uid(_))
        .sliding(2)
        .map(uids => UidRange(uids.head, uids.last))
        .zipWithIndex
        .map {
          case (range, idx) =>
            Partition(partition.targets.rotateLeft(idx), partition.operators + range)
        }
    }
  }

  private[connector] def getDenseAndSparsePartitions(uids: UnsignedLong): (UnsignedLong, UnsignedLong, UnsignedLong) = {
    // try to find dense and sparse sections and partition those with individual uids per partition
    // assumption is a dense region from uids [0x1 … denseUids] and a sparse region
    // from [denseUids+1 … uidCardinality] containing estimated sparseUids uids
    val (denseUids, sparseUids) = getDenseAndSparseUids(uids)

    // we put uidsPerPartition into dense and sparse partitions
    // when this requires more than maxPartitions, then we first reduce the number of sparse partitions down to 1
    // then we further reduce dense partitions down to maxPartitions-1 by increasing sparse and dense uidsPerpartitions
    val densePartitions = divideAndCeil(denseUids, uidsPerPartitionUnsigned)
    val sparsePartitions = divideAndCeil(sparseUids, uidsPerPartitionUnsigned)
    val allPartitions = densePartitions.plus(sparsePartitions)
    if (allPartitions.compareTo(maxPartitionsUnsigned) <= 0) {
      (densePartitions, sparsePartitions, denseUids)
    } else if (sparsePartitions.compareTo(UnsignedLong.ONE) > 0 && densePartitions.compareTo(maxPartitionsUnsigned) < 0) {
      val remainingPartitions = maxPartitionsUnsigned.minus(densePartitions)
      log.warn(s"Some partitions may contain more than $uidsPerPartition uids, " +
        s"otherwise more than $maxPartitions would be created. " +
        s"Increase $UidRangePartitionerMaxPartsOption to $allPartitions " +
        s"or $UidRangePartitionerUidsPerPartOption to ${divideAndCeil(sparseUids, remainingPartitions)}")
      (densePartitions, remainingPartitions, denseUids)
    } else {
      log.warn(s"Partitions will contain more than $uidsPerPartition uids, " +
        s"otherwise more than $maxPartitions would be created. " +
        s"Increase $UidRangePartitionerMaxPartsOption to $allPartitions " +
        s"or $UidRangePartitionerUidsPerPartOption to ${divideAndCeil(denseUids.plus(sparseUids), maxPartitionsUnsigned)}")
      val actualSparsePartitions = if (sparsePartitions.compareTo(UnsignedLong.ONE) <= 0) sparsePartitions else UnsignedLong.ONE
      (maxPartitionsUnsigned.minus(actualSparsePartitions), actualSparsePartitions, denseUids)
    }
  }
}
