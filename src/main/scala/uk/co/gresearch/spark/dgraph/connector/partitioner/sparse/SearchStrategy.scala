package uk.co.gresearch.spark.dgraph.connector.partitioner.sparse

import com.google.common.primitives.UnsignedLong

trait SearchStrategy {
  def middle(step: SearchStep[_, _]): Option[UnsignedLong]
}
