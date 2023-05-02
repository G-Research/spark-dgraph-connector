package uk.co.gresearch.spark.dgraph.connector.partitioner
import com.google.common.primitives.UnsignedLong
import uk.co.gresearch.spark.dgraph.connector.Uid

/**
 * A sequence of uids is considered sparse if there are no more than
 * denseGapRepetitionThreshold consecutive dense gaps.
 *
 * @param denseGapThreshold gaps below this threshold are considered dense
 * @param denseGapRepetitionThreshold observe consecutive dense gaps to consider uids dense
 */
case class ThresholdSparseDetector(denseGapThreshold: Int, denseGapRepetitionThreshold: Int) extends SparseDetector {
  if (denseGapRepetitionThreshold < 1) throw new IllegalArgumentException(s"The denseGapRepetitionThreshold must be larger than zero: $denseGapThreshold")
  private val denseGapThresholdUnsigned = UnsignedLong.valueOf(denseGapThreshold)

  override def isSparse(uids: Seq[Uid]): Boolean = {
    if (uids.length <= denseGapRepetitionThreshold) {
      false
    } else {
      val repetition = uids.map(_.uid)
        .sliding(2)
        .map(seq => seq.last.minus(seq.head))
        .foldLeft(0)((count, gap) => if (count >= denseGapRepetitionThreshold) count else
          if (gap.compareTo(denseGapThresholdUnsigned) > 0) count + 1 else 0)
      repetition >= denseGapRepetitionThreshold
    }
  }
}
