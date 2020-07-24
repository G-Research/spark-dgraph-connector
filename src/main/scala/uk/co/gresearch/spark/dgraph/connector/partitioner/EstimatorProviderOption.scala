package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.apache.spark.sql.sources.v2.DataSourceOptions
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, ConfigParser, MaxLeaseIdEstimatorOption, _}

trait EstimatorProviderOption extends ConfigParser with ClusterStateHelper with Logging {

  def getEstimatorOption(option: String, options: DataSourceOptions, default: String,
                         clusterState: ClusterState): UidCardinalityEstimator = {
    val name = getStringOption(option, options, default)
    name match {
      case MaxLeaseIdEstimatorOption =>
        val maxLeaseId = getIntOption(MaxLeaseIdEstimatorIdOption, options).map(_.toLong)
        maxLeaseId.foreach(id => log.warn(s"using configured maxLeaseId=$id for uid cardinality estimator"))
        UidCardinalityEstimator.forMaxLeaseId(maxLeaseId.getOrElse(clusterState.maxLeaseId))
      case _ => throw new IllegalArgumentException(s"Unknown uid cardinality estimator: $name")
    }
  }

}
