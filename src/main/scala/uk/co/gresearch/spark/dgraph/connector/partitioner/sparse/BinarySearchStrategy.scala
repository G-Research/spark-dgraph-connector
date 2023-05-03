package uk.co.gresearch.spark.dgraph.connector.partitioner.sparse

import com.google.common.primitives.UnsignedLong
import uk.co.gresearch.spark.dgraph.connector.TWO

case class BinarySearchStrategy(minWidth: Int) extends SearchStrategy {
  private val minRangeLengthUnsigned = UnsignedLong.valueOf(minWidth)

  def middle(step: SearchStep[_, _]): Option[UnsignedLong] =
    if (step.width.compareTo(minRangeLengthUnsigned) > 0) {
      Some(step.left.plus(step.right).dividedBy(TWO))
    } else {
      None
    }

}
