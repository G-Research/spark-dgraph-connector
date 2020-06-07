package uk.co.gresearch.spark.dgraph.connector.partitioner

import uk.co.gresearch.spark.dgraph.connector.{Partition, UidRange}

case class UidRangePartitioner(partitioner: Partitioner, partitioningFactor: Int, maxUids: Long) extends Partitioner {

  if (partitioner == null)
    throw new IllegalArgumentException("partitioner must not be null")

  if (partitioningFactor <= 0)
    throw new IllegalArgumentException(s"partitioningFactor must be larger than zero: $partitioningFactor")

  if (maxUids <= 0)
    throw new IllegalArgumentException(s"partitioningFactor must be larger than zero: $partitioningFactor")

  val partitions: Seq[Partition] = partitioner.getPartitions
  if (partitions.exists(_.uids.isDefined))
    throw new IllegalArgumentException(s"UidRangePartitioner cannot be combined with " +
      s"another uid partitioner: ${partitioner.getClass.getSimpleName}")

  override def getPartitions: Seq[Partition] = {
    if (partitioningFactor > 1) {
      val factor = Math.min(partitioningFactor, maxUids).toInt
      val uidRangeSize = ((maxUids - 1) / factor) + 1
      (0 until factor).map(idx => idx -> UidRange(idx*uidRangeSize, uidRangeSize)).flatMap {
        case (idx, range) =>
          partitions.map(partition =>
            Partition(partition.targets.rotate(idx), partition.predicates, Some(range))
          )
      }
    } else {
      partitions
    }
  }

}
