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

import uk.co.gresearch.spark.dgraph.connector.{Partition, UidRange}

case class UidRangePartitioner(partitioner: Partitioner, uidsPerPartition: Int, uidCardinality: Long) extends Partitioner {

  if (partitioner == null)
    throw new IllegalArgumentException("partitioner must not be null")

  if (uidsPerPartition <= 0)
    throw new IllegalArgumentException(s"uidsPerPartition must be larger than zero: $uidsPerPartition")

  if (uidCardinality <= 0)
    throw new IllegalArgumentException(s"uidCardinality must be larger than zero: $uidCardinality")

  val parts: Long = ((uidCardinality - 1) / uidsPerPartition) + 1
  if (!parts.isValidInt)
    throw new IllegalArgumentException(s"uidsPerPartition of $uidsPerPartition with uidCardinality of $uidCardinality leads to more then ${Integer.MAX_VALUE} partitions: $parts")

  val partitions: Seq[Partition] = partitioner.getPartitions

  if (partitions.exists(_.uids.isDefined))
    throw new IllegalArgumentException(s"UidRangePartitioner cannot be combined with " +
      s"another uid partitioner: ${partitioner.getClass.getSimpleName}")

  override def getPartitions: Seq[Partition] = {
    if (parts > 1) {
      (0 until parts.toInt).map(idx => idx -> UidRange(idx * uidsPerPartition, uidsPerPartition)).flatMap {
        case (idx, range) =>
          partitions.map(partition =>
            Partition(partition.targets.rotateLeft(idx), partition.predicates, Some(range))
          )
      }
    } else {
      partitions
    }
  }

}
