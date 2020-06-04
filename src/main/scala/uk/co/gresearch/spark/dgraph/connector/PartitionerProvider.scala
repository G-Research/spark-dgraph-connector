package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.partitioner.{Partitioner, SingletonPartitioner}

trait PartitionerProvider {

  def getPartitioner(targets: Seq[Target],
                     schema: Schema,
                     clusterState: ClusterState,
                     options: CaseInsensitiveStringMap): Partitioner = {
    new SingletonPartitioner(targets)
  }

}
