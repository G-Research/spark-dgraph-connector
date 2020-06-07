package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector._

class PredicatePartitionerOption extends PartitionerProviderOption with ConfigParser {
  override def getPartitioner(targets: Seq[Target],
                              schema: Schema,
                              clusterState: ClusterState,
                              options: CaseInsensitiveStringMap): Option[Partitioner] =
    Some(PredicatePartitioner(schema, clusterState,
      getIntOption(PredicatePartitionerPredicatesOption, options, PredicatePartitionerPredicatesDefault)))
}
