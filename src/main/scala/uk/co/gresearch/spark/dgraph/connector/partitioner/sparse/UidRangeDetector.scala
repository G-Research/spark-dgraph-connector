package uk.co.gresearch.spark.dgraph.connector.partitioner.sparse

import com.google.common.primitives.UnsignedLong
import uk.co.gresearch.spark.dgraph.connector.Partition

trait UidRangeDetector {
  /**
   * Detects the end of the first dense uid region.
   *
   * @return number of uids in the first dense region and smallest gap in sparse regions
   */
  def detectDenseRegion(partition: Partition, uids: UnsignedLong): (UnsignedLong, UnsignedLong)
}
