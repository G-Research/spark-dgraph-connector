package uk.co.gresearch.spark.dgraph.connector

import com.google.gson.{Gson, JsonObject, JsonPrimitive}
import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Response
import io.grpc.ManagedChannel
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import uk.co.gresearch.spark.dgraph.connector.encoder.TripleEncoder

import scala.collection.JavaConverters._
case class Nodes(nodes: Array[Map[String, Any]])
case class Triple(s: Long, p: String, o: String)

class DGraphTriplePartitionReader(partition: DGraphPartition, encoder: TripleEncoder) extends PartitionReader[InternalRow] {

  val query: String =
    """{
      |  nodes (func: has(dgraph.type)) {
      |    uid
      |    expand(_all_)
      |  }
      |}""".stripMargin

  val channels: Seq[ManagedChannel] = partition.targets.map(toChannel)
  val client: DgraphClient = getClientFromChannel(channels)
  val response: Response = client.newReadOnlyTransaction().query(query)
  val json: String = response.getJson.toStringUtf8

  val triples: Iterator[Triple] =
    new Gson().fromJson(json, classOf[JsonObject])
      .getAsJsonArray("nodes")
      .iterator()
      .asScala
      .map(_.getAsJsonObject)
      .flatMap(toTriples)

  private def toTriples(node: JsonObject): Iterator[Triple] = {
    val string = node.remove("uid").getAsString
    if (!string.startsWith("0x")) {
      throw new IllegalArgumentException("DGraph subject is not a long prefixed with '0x': " + string)
    }
    val uid = java.lang.Long.valueOf(string.substring(2), 16)
    node.entrySet().iterator().asScala.map(e => Triple(uid, e.getKey, e.getValue.getAsString))
  }

  def next: Boolean = triples.hasNext

  def get: InternalRow = encoder.asInternalRow(triples.next())

  def close(): Unit = channels.foreach(_.shutdown())

}
