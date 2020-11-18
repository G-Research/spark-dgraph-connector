/*
 * Copyright 2020 G-Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector.executor

import io.dgraph.DgraphClient
import io.grpc.ManagedChannel
import uk.co.gresearch.spark.dgraph.connector.{ExtendedThrowable, GraphQl, Json, Logging, Target, Transaction, getClientFromChannel, toChannel}

/**
 * A QueryExecutor implementation that executes a GraphQl query against a Dgraph cluster returning a Json result.
 * All queries are executed in the given transaction.
 *
 * @param transaction transaction
 * @param targets dgraph cluster targets
 */
case class DgraphExecutor(transaction: Option[Transaction], targets: Seq[Target]) extends JsonGraphQlExecutor with Logging {

  private def getTransaction(client: DgraphClient): io.dgraph.Transaction =
    transaction.fold(client.newReadOnlyTransaction())(txn => client.newReadOnlyTransaction(txn.context))

  /**
   * Executes a GraphQl query against a Dgraph cluster and returns the JSON query result.
   *
   * @param query: the query
   * @return a Json result
   */
  override def query(query: GraphQl): Json = {
    log.trace(s"querying dgraph cluster at [${targets.mkString(",")}] with:\n${abbreviate(query.string)}")

    val channels: Seq[ManagedChannel] = targets.map(toChannel)
    try {
      val client = getClientFromChannel(channels)
      val response = getTransaction(client).query(query.string)
      val json = response.getJson.toStringUtf8

      if (log.isTraceEnabled)
        log.trace(s"retrieved response of ${loggingFormat.format(json.getBytes.length)} bytes: ${abbreviate(json)}")

      Json(json)
    } catch {
      case e: Throwable =>
        // this is potentially an async exception which does not include any useful stacktrace, so we add it here
        val exc = e.fillInStackTrace()

        // provide a useful exception when we see an RESOURCE_EXHAUSTED gRPC code
        if (exc.causedByResourceExhausted())
          throw new RuntimeException("Query failed because the result is too large. " +
            "Try a different chunk size or partition configuration: " +
            "https://github.com/g-research/spark-dgraph-connector#streamed-partitions and " +
            "https://github.com/g-research/spark-dgraph-connector#partitioning",
            exc
          )

        // just rethrow any other exception
        throw exc
    } finally {
      channels.foreach(_.shutdown())
    }
  }

}
