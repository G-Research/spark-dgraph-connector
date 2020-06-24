package uk.co.gresearch.spark.dgraph.connector.executor
import com.google.gson.{Gson, JsonObject}
import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Response
import io.grpc.ManagedChannel
import org.apache.spark.TaskContext
import uk.co.gresearch.spark.dgraph.connector.{GraphQl, Json, Partition, PerfJson, getClientFromChannel, toChannel}

case class DgraphPerfExecutor(partition: Partition) extends JsonGraphQlExecutor {

  /**
   * Executes the given graphql query and returns the query result as json.
   *
   * @param query query
   * @return result
   */
  override def query(query: GraphQl): Json = {
    val channels: Seq[ManagedChannel] = partition.targets.map(toChannel)
    try {
      val client: DgraphClient = getClientFromChannel(channels)
      val response: Response = client.newReadOnlyTransaction().query(query.string)
      val perf = getPerf(query, response)

      // return the response as json as usual, but add some perf payload to the first element
      // if there is no first element, create one, it will be ignored anyway
      val gson = new Gson()
      val perfJson = gson.fromJson(gson.toJson(perf), classOf[JsonObject])
      val jsonObj = gson.fromJson(response.getJson.toStringUtf8, classOf[JsonObject])
      val jsonArr = jsonObj.getAsJsonArray(query.resultName)
      if (jsonArr.size() == 0) {
        val obj = new JsonObject()
        obj.add("_spark_dgraph_conenctor_perf", perfJson)
        jsonArr.add(obj)
      } else {
        jsonArr.get(0).getAsJsonObject.add("_spark_dgraph_conenctor_perf", perfJson)
      }
      Json(gson.toJson(jsonObj))
    } finally {
      channels.foreach(_.shutdown())
    }
  }

  def getPerf(query: GraphQl, response: Response): PerfJson = {
    val task = TaskContext.get()
    val latency = Some(response).filter(_.hasLatency).map(_.getLatency)
    new PerfJson(
      partition.targets.map(_.target).toArray,
      partition.predicates.toArray,
      partition.uidRange.map(_.first.uid.asInstanceOf[java.lang.Long]).orNull,
      partition.uidRange.map(_.length.asInstanceOf[java.lang.Long]).orNull,

      query.chunk.map(_.after.uid.asInstanceOf[java.lang.Long]).orNull,
      query.chunk.map(_.length.asInstanceOf[java.lang.Long]).orNull,

      task.stageId(),
      task.stageAttemptNumber(),
      task.partitionId(),
      task.attemptNumber(),
      task.taskAttemptId(),

      latency.map(_.getAssignTimestampNs.asInstanceOf[java.lang.Long]).orNull,
      latency.map(_.getParsingNs.asInstanceOf[java.lang.Long]).orNull,
      latency.map(_.getProcessingNs.asInstanceOf[java.lang.Long]).orNull,
      latency.map(_.getEncodingNs.asInstanceOf[java.lang.Long]).orNull,
      latency.map(_.getTotalNs.asInstanceOf[java.lang.Long]).orNull
    )
  }

}
