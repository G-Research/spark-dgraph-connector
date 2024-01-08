package uk.co.gresearch.spark.dgraph.connector.partitioner.sparse

import com.google.common.primitives.UnsignedLong

case class SparsityEstimate(estimate: Float = 0, weight: Int = 0) {
  def update(gaps: Seq[UnsignedLong]): SparsityEstimate = {
    if (gaps.nonEmpty) {
      val median = gaps.sorted.apply(gaps.length / 2)
      SparsityEstimate((estimate * weight + median.floatValue()) / (weight + 1), weight + 1)
    } else {
      this
    }
  }
}
