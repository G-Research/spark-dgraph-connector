package uk.co.gresearch.spark.dgraph.connector

import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Response
import io.grpc.ManagedChannel

class JsonGraphQlExecutor(targets: Seq[Target]) extends GraphQlExecutor[Json] {

  /**
   * Executes the given query of type Q and returns the query result of type R.
   *
   * @param query query
   * @return result
   */
  override def query(query: GraphQl): Json = {
    val channels: Seq[ManagedChannel] = targets.map(toChannel)
    try {
      val client: DgraphClient = getClientFromChannel(channels)
      val response: Response = client.newReadOnlyTransaction().query(query.string)
      Json(response.getJson.toStringUtf8)
    } finally {
      channels.foreach(_.shutdown())
    }
  }

}
