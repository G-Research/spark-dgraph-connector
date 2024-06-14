package uk.co.gresearch.spark.dgraph.connector.partitioner.sparse

import com.google.common.primitives.UnsignedLong
import uk.co.gresearch.spark.dgraph.connector.{Partition, Uid}

case class SearchUidRangeDetector(
    uidDetector: UidDetector,
    sparseDetector: SparseDetector,
    searchLowerBound: Int,
    detectLength: Int,
    strategy: SearchStrategy
) extends UidRangeDetector
    with Search[Payload, Result] {

  private val searchLowerBoundUnsigned = UnsignedLong.valueOf(searchLowerBound)

  /**
   * Detects the end of the first dense uid region.
   *
   * @return
   *   first uid in the first sparse region
   */
  override def detectDenseRegion(partition: Partition, uids: UnsignedLong): (UnsignedLong, UnsignedLong) = {
    val payload = Payload(partition, SparsityEstimate())
    val step = UidRangeSearchStep(uidDetector, sparseDetector, detectLength, searchLowerBoundUnsigned, uids, payload)
    val result = search(step)
    (result.uid, result.estimate)
  }

}

case class Payload(partition: Partition, sparsityEstimate: SparsityEstimate)
case class Result(uid: UnsignedLong, estimate: UnsignedLong)
