package uk.co.gresearch.spark.dgraph.connector.partitioner.sparse
import com.google.common.primitives.UnsignedLong
import uk.co.gresearch.spark.dgraph.connector.partitioner.sparse.LogSearchStrategy.{bits, longBitSet}

case class LogSearchStrategy() extends SearchStrategy {
  override def middle(step: SearchStep[_, _]): Option[UnsignedLong] = {
    val leftBits = bits(step.left)
    val rightBits = bits(step.right)
    if (leftBits < rightBits) {
      val middleBits = (leftBits + rightBits) / 2
      val middle = longBitSet(middleBits.byteValue())
      if (step.left.compareTo(middle) < 0 && step.right.compareTo(middle) > 0) Some(middle) else None
    } else {
      None
    }
  }
}

object LogSearchStrategy {
  private val log2: Double = math.log(2)

  def bits(long: UnsignedLong): Byte =
    if (long.longValue() < 0) 64 else (math.log(long.longValue()) / log2 + 1).byteValue()

  def longBitSet(bits: Byte): UnsignedLong =
    if (bits <= 63) {
      UnsignedLong.valueOf((1L << bits) - 1)
    } else {
      UnsignedLong.MAX_VALUE
    }
}
