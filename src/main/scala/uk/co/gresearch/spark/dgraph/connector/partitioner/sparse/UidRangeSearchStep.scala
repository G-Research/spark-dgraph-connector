package uk.co.gresearch.spark.dgraph.connector.partitioner.sparse

import com.google.common.primitives.UnsignedLong
import uk.co.gresearch.spark.dgraph.connector.{Logging, Partition, Uid}

case class UidRangeSearchStep(
    uidDetector: UidDetector,
    sparseDetector: SparseDetector,
    detectLength: Int,
    override val left: UnsignedLong,
    override val right: UnsignedLong,
    override val payload: Payload
) extends SearchStep[Payload, Result]
    with Logging {

  override def move(middle: UnsignedLong): UidRangeSearchStep = {
    val range = uidDetector.getUids(payload.partition, Uid(middle), detectLength)
    if (range.isEmpty) {
      log.debug(s"no uids after $middle")
      copy(left = left, right = middle)
    } else if (sparseDetector.isSparse(range)) {
      log.debug(abbreviate(s"sparse gaps after $middle: ${sparseDetector.getSparseGaps(range).mkString(", ")}", 100))
      val sparsity = payload.sparsityEstimate.update(sparseDetector.getSparseGaps(range))
      copy(left = left, right = middle, payload = payload.copy(sparsityEstimate = sparsity))
    } else {
      log.debug(abbreviate(s"dense uids after $middle: ${sparseDetector.getSparseGaps(range).mkString(", ")}", 100))
      copy(left = middle, right = right)
    }
  }

  override def result(): Result = {
    val long = payload.sparsityEstimate.estimate.longValue()
    Result(right, UnsignedLong.valueOf(long))
  }
}
