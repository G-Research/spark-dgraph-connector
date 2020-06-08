package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector._

class DefaultPartitionerOption extends PartitionerProviderOption with ConfigParser {
  override def getPartitioner(schema: Schema,
                              clusterState: ClusterState,
                              options: CaseInsensitiveStringMap): Option[Partitioner] =
    Some(
      UidRangePartitioner(
        SingletonPartitioner(allClusterTargets(clusterState)),
        getIntOption(UidRangePartitionerUidsPerPartOption, options, UidRangePartitionerUidsPerPartDefault),
        clusterState.maxLeaseId
      )
    )
}
