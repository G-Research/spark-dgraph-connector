package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector._

trait EstimatorProviderOption extends ConfigParser with ClusterStateHelper {

  def getEstimatorOption(option: String, options: CaseInsensitiveStringMap, default: String,
                         clusterState: ClusterState): UidCardinalityEstimator = {
    val name = getStringOption(option, options, default)
    name match {
      case MaxLeaseIdEstimatorOption =>
        val maxLeaseId = getIntOption(MaxLeaseIdEstimatorIdOption, options).map(_.toLong)
        maxLeaseId.foreach(id => println(s"WARN: using configured maxLeaseId=$id for uid cardinality estimator"))
        UidCardinalityEstimator.forMaxLeaseId(maxLeaseId.getOrElse(clusterState.maxLeaseId))
      case _ => throw new IllegalArgumentException(s"Unknown uid cardinality estimator: $name")
    }
  }

}
