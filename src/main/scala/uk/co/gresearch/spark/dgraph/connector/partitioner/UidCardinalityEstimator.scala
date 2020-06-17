package uk.co.gresearch.spark.dgraph.connector.partitioner

import com.google.gson.{Gson, JsonObject}
import uk.co.gresearch.spark.dgraph.connector.executor.JsonGraphQlExecutor
import uk.co.gresearch.spark.dgraph.connector.{Json, Partition, UidRange}

import scala.collection.JavaConverters._

trait UidCardinalityEstimator {

  /**
   * Estimates the cardinality of uids in the given partition,
   * or None if an estimation is not available.
   *
   * @param partition a partition
   * @return estimated number of uids or None
   */
  def uidCardinality(partition: Partition): Option[Long]

}

/**
 * A base implementation of UidCardinalityEstimator.
 */
abstract class UidCardinalityEstimatorBase extends UidCardinalityEstimator {

  /**
   * Estimates the cardinality of uids in the given partition,
   * or None if an estimation is not available.
   *
   * @param partition a partition
   * @return estimated number of uids or None
   */
  override def uidCardinality(partition: Partition): Option[Long] = partition match {
    case Partition(_, _, Some(UidRange(_, length))) => Some(length)
    case _ => None
  }

}

case class MaxLeaseIdUidCardinalityEstimator(maxLeaseId: Long) extends UidCardinalityEstimatorBase {

  if (maxLeaseId <= 0)
    throw new IllegalArgumentException(s"uidCardinality must be larger than zero: $maxLeaseId")

  /**
   * Estimates the cardinality of uids in the given partition,
   * or None if an estimation is not available.
   *
   * @param partition a partition
   * @return estimated number of uids or None
   */
  override def uidCardinality(partition: Partition): Option[Long] =
    super.uidCardinality(partition).orElse(Some(maxLeaseId))

}

case class QueryUidCardinalityEstimator(executor: JsonGraphQlExecutor) extends UidCardinalityEstimatorBase {

  /**
   * Estimates the cardinality of uids in the given partition,
   * or None if an estimation is not available.
   *
   * @param partition a partition
   * @return estimated number of uids or None
   */
  override def uidCardinality(partition: Partition): Option[Long] = {
    super.uidCardinality(partition).orElse(queryUidCardinality(partition))
  }

  def queryUidCardinality(partition: Partition): Option[Long] = {
    val query = partition.query
    val graphql = query.countUids
    println(graphql.string)
    val json = executor.query(graphql)
    println(json.string)
    getCardinality(json, query.resultName)
  }

  def getCardinality(json: Json, member: String): Option[Long] = {
    val cardinality =
      new Gson().fromJson(json.string, classOf[JsonObject])
        .getAsJsonArray(member)
        .iterator()
        .asScala
        .map(_.getAsJsonObject.getAsJsonPrimitive("count").getAsLong)
        .toSeq

    if (cardinality.size > 1) {
      println(s"retrieved multiple cardinality information:\n${json.string}")
      None
    } else {
      cardinality.headOption
    }
  }

}

object UidCardinalityEstimator {
  def forMaxLeaseId(maxLeaseId: Long): UidCardinalityEstimator =
    MaxLeaseIdUidCardinalityEstimator(maxLeaseId)
  def forExecutor(executor: JsonGraphQlExecutor): UidCardinalityEstimator =
    QueryUidCardinalityEstimator(executor)
}
