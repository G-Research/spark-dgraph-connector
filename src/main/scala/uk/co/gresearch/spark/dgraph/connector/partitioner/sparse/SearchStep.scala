package uk.co.gresearch.spark.dgraph.connector.partitioner.sparse

import com.google.common.primitives.UnsignedLong

trait SearchStep[T, R] {
  val left: UnsignedLong
  val right: UnsignedLong
  val payload: T

  lazy val width: UnsignedLong = right.minus(left)

  def move(middle: UnsignedLong): SearchStep[T, R]
  def result(): R
}
