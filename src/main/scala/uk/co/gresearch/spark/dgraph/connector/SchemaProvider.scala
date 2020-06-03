package uk.co.gresearch.spark.dgraph.connector

import com.google.gson.{Gson, JsonObject}
import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Response
import io.grpc.ManagedChannel

import scala.collection.JavaConverters._

trait SchemaProvider {

  private val query = "schema { predicate type }"

  def getSchema(targets: Seq[Target]): Schema = {
    val channels: Seq[ManagedChannel] = targets.map(toChannel)
    try {
      val client: DgraphClient = getClientFromChannel(channels)
      val response: Response = client.newReadOnlyTransaction().query(query)
      val json: String = response.getJson.toStringUtf8
      val schema = new Gson().fromJson(json, classOf[JsonObject])
        .get("schema").getAsJsonArray.asScala
        .map(_.getAsJsonObject)
        .map(o => o.get("predicate").getAsString -> o.get("type").getAsString)
        .toMap
      Schema(schema)
    } finally {
      channels.foreach(_.shutdown())
    }
  }

}
