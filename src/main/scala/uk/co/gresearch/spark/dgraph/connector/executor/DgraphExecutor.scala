package uk.co.gresearch.spark.dgraph.connector.executor

import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Response
import io.grpc.ManagedChannel
import uk.co.gresearch.spark.dgraph.connector.{GraphQl, Json, Target, getClientFromChannel, toChannel}

case class DgraphExecutor(targets: Seq[Target]) extends JsonGraphQlExecutor {

  /**
   * Executes a GraphQl query against a Dgraoh cluster and returns the JSON query result.
   *
   * @param query: the query
   * @return a Json result
   */
  override def query(query: GraphQl): Json = {
    val channels: Seq[ManagedChannel] = targets.map(toChannel)
    try {
      val client: DgraphClient = getClientFromChannel(channels)
      println(query.string)
      val response: Response = client.newReadOnlyTransaction().query(query.string)
      Json(response.getJson.toStringUtf8)
    } catch {
      // this is potentially a async exception which does not include any useful stacktrace, so we add it here
      case e: Throwable => throw e.fillInStackTrace()
    } finally {
      channels.foreach(_.shutdown())
    }
  }

}
