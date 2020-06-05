package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Schema, Target}

trait PartitionerProvider {

  val partitionerOptions = Seq(
    new ConfigPartitionerOption(),
    new PredicatePartitionerOption(),
    new DefaultPartitionerOption()
  )

  def getPartitioner(targets: Seq[Target],
                     schema: Schema,
                     clusterState: ClusterState,
                     options: CaseInsensitiveStringMap): Partitioner =
    partitionerOptions
      .flatMap(_.getPartitioner(targets, schema, clusterState, options))
      .headOption
      .getOrElse(throw new RuntimeException("Could not find any suitable partitioner"))

}
