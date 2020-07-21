package uk.co.gresearch.spark.dgraph.connector.partitioner

import uk.co.gresearch.spark.dgraph.connector.Partition

trait UidCardinalityEstimator {

  /**
   * Estimates the cardinality of uids in the given partition,
   * or None if an estimation is not available.
   *
   * @param partition a partition
   * @return estimated number of uids or None
   */
  def uidCardinality(partition: Partition): Option[Long]

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
  override def uidCardinality(partition: Partition): Option[Long] =
    partition.uids.map(_.length)

}

case class MaxLeaseIdUidCardinalityEstimator(maxLeaseId: Long) extends UidCardinalityEstimatorBase {

  if (maxLeaseId <= 0)
    throw new IllegalArgumentException(s"uidCardinality must be larger than zero: $maxLeaseId")

  /**
   * Estimates the cardinality of uids in the given partition,
   * or None if an estimation is not available.
   *
   * @param partition a partition
   * @return estimated number of uids or None
   */
  override def uidCardinality(partition: Partition): Option[Long] =
    super.uidCardinality(partition).orElse(Some(maxLeaseId))

}

object UidCardinalityEstimator {
  def forMaxLeaseId(maxLeaseId: Long): UidCardinalityEstimator =
    MaxLeaseIdUidCardinalityEstimator(maxLeaseId)
}
