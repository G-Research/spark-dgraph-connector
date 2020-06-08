package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector._

class ConfigPartitionerOption extends PartitionerProviderOption with ConfigParser {

  override def getPartitioner(schema: Schema,
                              clusterState: ClusterState,
                              options: CaseInsensitiveStringMap): Option[Partitioner] =
    getStringOption(PartitionerOption, options)
      .map(getPartitioner(_, schema, clusterState, options))

  private def getPartitioner(partitionerName: String,
                             schema: Schema,
                             clusterState: ClusterState,
                             options: CaseInsensitiveStringMap): Partitioner =
    partitionerName match {
      case SingletonPartitionerOption => SingletonPartitioner(allClusterTargets(clusterState))
      case GroupPartitionerOption => GroupPartitioner(schema, clusterState)
      case AlphaPartitionerOption =>
        AlphaPartitioner(schema, clusterState,
          getIntOption(AlphaPartitionerPartitionsOption, options, AlphaPartitionerPartitionsDefault))
      case PredicatePartitionerOption =>
        PredicatePartitioner(schema, clusterState,
          getIntOption(PredicatePartitionerPredicatesOption, options, PredicatePartitionerPredicatesDefault))
      case UidRangePartitionerOption =>
        val uidsPerPartition = getIntOption(UidRangePartitionerUidsPerPartOption, options, UidRangePartitionerUidsPerPartDefault)
        val targets = allClusterTargets(clusterState)
        val singleton = SingletonPartitioner(targets)
        UidRangePartitioner(singleton, uidsPerPartition, clusterState.maxLeaseId)
      case option if option.endsWith(s"+${UidRangePartitionerOption}") =>
        val name = option.substring(0, option.indexOf('+'))
        val partitioner = getPartitioner(name, schema, clusterState, options)
        getPartitioner(UidRangePartitionerOption, schema, clusterState, options)
          .asInstanceOf[UidRangePartitioner].copy(partitioner = partitioner)
      case unknown => throw new IllegalArgumentException(s"Unknown partitioner: $unknown")
    }

}
