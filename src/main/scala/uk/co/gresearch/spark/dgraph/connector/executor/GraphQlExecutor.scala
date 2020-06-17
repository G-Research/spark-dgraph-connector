package uk.co.gresearch.spark.dgraph.connector.executor

import uk.co.gresearch.spark.dgraph.connector.GraphQl

/**
 * Executes a GraphQl query and returns the query result of type R.
 *
 * @tparam R type of the query result
 */
trait GraphQlExecutor[R] extends QueryExecutor[GraphQl, R]
