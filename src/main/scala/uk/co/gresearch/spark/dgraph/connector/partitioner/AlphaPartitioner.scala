package uk.co.gresearch.spark.dgraph.connector.partitioner

import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Partition, Schema}

case class AlphaPartitioner(schema: Schema, clusterState: ClusterState, partitionsPerAlpha: Int)
  extends Partitioner with ClusterStateHelper {

  if (partitionsPerAlpha <= 0)
    throw new IllegalArgumentException(s"partitionsPerAlpha must be larger than zero: $partitionsPerAlpha")

  override def getPartitions: Seq[Partition] = {
    PredicatePartitioner.getPartitions(
      schema, clusterState, getGroupTargets(clusterState, _).size * partitionsPerAlpha
    )
  }

}
