package uk.co.gresearch.spark.dgraph.connector.partitioner.sparse

import com.google.common.primitives.UnsignedLong
import uk.co.gresearch.spark.dgraph.connector.Partition

case class NoUidRangeDetector() extends UidRangeDetector {
  /**
   * Detects the end of the first dense uid region.
   *
   * @return first uid in the first sparse region and smallest gap in sparse regions
   */
  override def detectDenseRegion(partition: Partition, uids: UnsignedLong): (UnsignedLong, UnsignedLong) =
    (uids, UnsignedLong.MAX_VALUE)
}
