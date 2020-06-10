package uk.co.gresearch.spark.dgraph.connector

trait QueryExecutor[Q,R] {
  /**
   * Executes the given query of type Q and returns the query result of type R.
   *
   * @param query query
   * @return result
   */
  def query(query: Q): R
}
