package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, PartitionerOption, Schema, SingletonPartitionerOption, Target}

class ConfigPartitionerOption extends PartitionerProviderOption {
  override def getPartitioner(targets: Seq[Target],
                              schema: Schema,
                              clusterState: ClusterState,
                              options: CaseInsensitiveStringMap): Option[Partitioner] =
    Option(options.get(PartitionerOption)).flatMap {
      case SingletonPartitionerOption => Some(new SingletonPartitioner(targets))
      case _ => None
    }
}
