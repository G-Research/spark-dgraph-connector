package uk.co.gresearch.spark.dgraph.connector.executor

import uk.co.gresearch.spark.dgraph.connector.Json

/**
 * Executes a GraphQl query and returns the query result of type R.
 */
trait JsonGraphQlExecutor extends GraphQlExecutor[Json]
