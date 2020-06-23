package uk.co.gresearch.spark.dgraph.connector.executor
import uk.co.gresearch.spark.dgraph.connector.Partition

case class DgraphExecutorProvider() extends ExecutorProvider {

  /**
   * Provide an executor for the given partition.
   *
   * @param partition a partitioon
   * @return an executor
   */
  override def getExecutor(partition: Partition): JsonGraphQlExecutor =
    DgraphExecutor(partition.targets)

}
