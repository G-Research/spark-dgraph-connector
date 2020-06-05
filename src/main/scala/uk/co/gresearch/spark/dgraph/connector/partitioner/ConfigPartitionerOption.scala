package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector._

class ConfigPartitionerOption extends PartitionerProviderOption with ConfigParser {
  override def getPartitioner(targets: Seq[Target],
                              schema: Schema,
                              clusterState: ClusterState,
                              options: CaseInsensitiveStringMap): Option[Partitioner] =
    Option(options.get(PartitionerOption)).flatMap {
      case SingletonPartitionerOption => Some(new SingletonPartitioner(targets))
      case GroupPartitionerOption => Some(new GroupPartitioner(schema, clusterState))
      case AlphaPartitionerOption =>
        Some(new AlphaPartitioner(schema, clusterState,
          getIntOption(AlphaPartitionerPartitionsOption, options, AlphaPartitionerPartitionsDefault)))
      case PredicatePartitionerOption =>
        Some(new PredicatePartitioner(schema, clusterState,
          getIntOption(PredicatePartitionerPredicatesOption, options, PredicatePartitionerPredicatesDefault)))
      case _ => None
    }

}
