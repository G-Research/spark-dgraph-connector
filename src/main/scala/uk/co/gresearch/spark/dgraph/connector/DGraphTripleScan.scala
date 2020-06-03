package uk.co.gresearch.spark.dgraph.connector

import com.google.gson.{Gson, JsonObject}
import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Response
import io.grpc.ManagedChannel
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.encoder.TripleEncoder

import scala.collection.JavaConverters._

class DGraphTripleScan(targets: Seq[Target], encoder: TripleEncoder) extends Scan with Batch {

  override def readSchema(): StructType = encoder.readSchema()

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(DGraphPartition(targets, getSchema))
  }

  override def createReaderFactory(): PartitionReaderFactory = new DGraphTriplePartitionReaderFactory(encoder)

  private val query = "schema { predicate type }"

  def getSchema: Schema = {
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
