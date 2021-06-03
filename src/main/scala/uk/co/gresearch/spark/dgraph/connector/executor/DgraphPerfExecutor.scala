package uk.co.gresearch.spark.dgraph.connector.executor
import com.google.gson.{Gson, JsonObject}
import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Response
import io.grpc.ManagedChannel
import org.apache.spark.TaskContext
import uk.co.gresearch.spark.dgraph.connector.sources.PerfSource
import uk.co.gresearch.spark.dgraph.connector.{GraphQl, Json, Partition, PerfJava, getClientFromChannel, toChannel}

case class DgraphPerfExecutor(partition: Partition) extends JsonGraphQlExecutor {

  val gson = new Gson()

  /**
   * Executes the given graphql query and returns the query result as json.
   *
   * @param query query
   * @return result
   */
  override def query(query: GraphQl): Json = {
    val channels: Seq[ManagedChannel] = partition.targets.map(toChannel)
    try {
      val startTs = System.nanoTime()
      val client: DgraphClient = getClientFromChannel(channels)
      val response: Response = client.newReadOnlyTransaction().query(query.string)
      val bytes = response.getJson
      val retrievedTs = System.nanoTime()

      // return the response as json as usual, but add some perf payload to the first element
      val jsonObj = gson.fromJson(bytes.toStringUtf8, classOf[JsonObject])
      val jsonArr = jsonObj.getAsJsonArray(query.resultName)
      val decodedTs = System.nanoTime()

      val wireMillis = retrievedTs - startTs
      val decodeMillis = decodedTs - retrievedTs
      val perf = getPerf(query, response, wireMillis, decodeMillis, jsonArr.size())
      if (jsonArr.size() > 0) {
        val perfJson = gson.fromJson(gson.toJson(perf), classOf[JsonObject])
        jsonArr.get(0).getAsJsonObject.add(PerfSource.perfElementName, perfJson)
      }
      Json(gson.toJson(jsonObj), Some(bytes.size()))
    } finally {
      channels.foreach(_.shutdown())
    }
  }

  def getPerf(query: GraphQl, response: Response, wireMillis: Long, decodeMillis: Long, nodes: Int): PerfJava = {
    val task = TaskContext.get()
    val latency = Some(response).filter(_.hasLatency).map(_.getLatency)
    new PerfJava(
      partition.targets.map(_.target).toArray,
      Some(partition.predicates.toSeq).map(_.toArray).orNull,
      partition.uidRange.map(_.first.uid.asInstanceOf[java.lang.Long]).orNull,
      partition.uidRange.map(_.length.asInstanceOf[java.lang.Long]).orNull,

      query.chunk.map(_.after.uid.asInstanceOf[java.lang.Long]).orNull,
      query.chunk.map(_.length.asInstanceOf[java.lang.Long]).orNull,
      response.getJson.size(),
      nodes,

      task.stageId(),
      task.stageAttemptNumber(),
      task.partitionId(),
      task.attemptNumber(),
      task.taskAttemptId(),

      latency.map(_.getAssignTimestampNs.asInstanceOf[java.lang.Long]).orNull,
      latency.map(_.getParsingNs.asInstanceOf[java.lang.Long]).orNull,
      latency.map(_.getProcessingNs.asInstanceOf[java.lang.Long]).orNull,
      latency.map(_.getEncodingNs.asInstanceOf[java.lang.Long]).orNull,
      latency.map(_.getTotalNs.asInstanceOf[java.lang.Long]).orNull,
      wireMillis,
      decodeMillis
    )
  }

}
