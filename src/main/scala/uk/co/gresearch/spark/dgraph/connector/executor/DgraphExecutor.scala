package uk.co.gresearch.spark.dgraph.connector.executor

import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Response
import io.grpc.ManagedChannel
import uk.co.gresearch.spark.dgraph.connector.{GraphQl, Json, Logging, Target, getClientFromChannel, toChannel}

case class DgraphExecutor(targets: Seq[Target]) extends JsonGraphQlExecutor with Logging {

  /**
   * Executes a GraphQl query against a Dgraoh cluster and returns the JSON query result.
   *
   * @param query: the query
   * @return a Json result
   */
  override def query(query: GraphQl): Json = {
    log.trace(s"querying dgraph cluster at [${targets.mkString(",")}] with:\n${abbreviate(query.string)}")

    val channels: Seq[ManagedChannel] = targets.map(toChannel)
    try {
      val client: DgraphClient = getClientFromChannel(channels)
      val response: Response = client.newReadOnlyTransaction().query(query.string)
      val json = response.getJson.toStringUtf8

      if (log.isTraceEnabled)
        log.trace(s"retrieved response of ${loggingFormat.format(json.getBytes.length)} bytes: ${abbreviate(json)}")

      Json(json)
    } catch {
      // this is potentially a async exception which does not include any useful stacktrace, so we add it here
      case e: Throwable => throw e.fillInStackTrace()
    } finally {
      channels.foreach(_.shutdown())
    }
  }

}
