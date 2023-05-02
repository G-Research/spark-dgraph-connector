package uk.co.gresearch.spark.dgraph.connector.partitioner
import com.google.common.primitives.UnsignedLong
import uk.co.gresearch.spark.dgraph.connector.{Partition, Uid}

case class LogBinarySearchUidRangeDetector(uidDetector: UidDetector, sparseDetector: SparseDetector, detectLength: Int, minRangeLength: Int) extends UidRangeDetector {
  private val TWO = UnsignedLong.valueOf(2)
  private val minRangeLengthUnsigned = UnsignedLong.valueOf(minRangeLength)

  /**
   * Detects the end of the first dense uid region.
   *
   * @return first uid in the first sparse region
   */
  override def detectDenseRegion(partition: Partition, uids: UnsignedLong): (UnsignedLong, UnsignedLong) = {
    val left = UnsignedLong.ONE
    val right = uids
    val (uid, min) = search(partition, left, right)
    (uid, min)
  }

  def search(partition: Partition, left: UnsignedLong, right: UnsignedLong, minSparseGap: UnsignedLong = UnsignedLong.MAX_VALUE): (UnsignedLong, UnsignedLong) = {
    if (left.minus(right).compareTo(minRangeLengthUnsigned) <= 0) {
      (right, minSparseGap)
    } else {
      val mid = middle(left, right)
      val range = uidDetector.getUids(partition, Uid(mid), detectLength)
      if (range.isEmpty) {
        search(partition, left, mid)
      } else if (range.last.uid.compareTo(right) >= 0) {
        (right, minSparseGap)
      } else {
        if (sparseDetector.isSparse(range))
          search(partition, left, mid, getMinSparseGap(minSparseGap, range))
        else
          search(partition, range.last.uid, right, minSparseGap)
      }
    }
  }

  def getMinSparseGap(minSparseGap: UnsignedLong, uids: Seq[Uid]): UnsignedLong = {
    if (uids.length < 2) {
      minSparseGap
    } else {
      Seq(minSparseGap, sparseDetector.getSparseGaps(uids)).min
    }
  }

  def middle(left: UnsignedLong, right: UnsignedLong): UnsignedLong = {
    left.plus(right).dividedBy(TWO)
  }
}
