package uk.co.gresearch.spark.dgraph.connector.partitioner

import com.google.common.primitives.UnsignedLong
import uk.co.gresearch.spark.dgraph.connector.Uid

trait SparseDetector {
  def isSparse(uids: Seq[Uid]): Boolean

  def getSparseGaps(uids: Seq[Uid]): UnsignedLong =
    uids.map(_.uid)
      .sliding(2)
      .map(seq => seq.last.minus(seq.head))
      .min
}
