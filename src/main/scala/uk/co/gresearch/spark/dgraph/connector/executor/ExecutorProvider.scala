package uk.co.gresearch.spark.dgraph.connector.executor

import uk.co.gresearch.spark.dgraph.connector.Partition

trait ExecutorProvider {

  /**
   * Provide an executor for the given partition.
   * @param partition a partitioon
   * @return an executor
   */
  def getExecutor(partition: Partition): JsonGraphQlExecutor

}
