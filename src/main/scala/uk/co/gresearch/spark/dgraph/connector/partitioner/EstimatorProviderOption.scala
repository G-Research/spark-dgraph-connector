package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.apache.spark.sql.sources.v2.DataSourceOptions
import uk.co.gresearch.spark.dgraph.connector.executor.DgraphExecutor
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, ConfigParser, MaxLeaseIdEstimatorOption, UidCountEstimatorOption}

trait EstimatorProviderOption extends ConfigParser with ClusterStateHelper {

  def getEstimatorOption(option: String, options: DataSourceOptions, default: String,
                         clusterState: ClusterState): UidCardinalityEstimator = {
    val name = getStringOption(option, options, default)
    name match {
      case MaxLeaseIdEstimatorOption => UidCardinalityEstimator.forMaxLeaseId(clusterState.maxLeaseId)
      case UidCountEstimatorOption => UidCardinalityEstimator.forExecutor(new DgraphExecutor(getAllClusterTargets(clusterState)))
      case _ => throw new IllegalArgumentException(s"Unknown uid cardinality estimator: $name")
    }
  }

}
