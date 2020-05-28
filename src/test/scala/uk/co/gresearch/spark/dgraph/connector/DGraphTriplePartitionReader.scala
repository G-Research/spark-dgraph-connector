package uk.co.gresearch.spark.dgraph.connector

import com.google.gson.{Gson, JsonObject, JsonPrimitive}
import io.dgraph.DgraphClient
import io.dgraph.DgraphProto.Response
import io.grpc.ManagedChannel
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
case class Nodes(nodes: Array[Map[String, Any]])
case class Triple(s: String, p: String, o: String) {
  def asInternalRow: InternalRow =
    InternalRow(
      UTF8String.fromString(s),
      UTF8String.fromString(p),
      UTF8String.fromString(o)
    )
}

class DGraphTriplePartitionReader(partition: DGraphPartition) extends PartitionReader[InternalRow] {

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
    val uid = node.remove("uid").getAsString
    node.entrySet().iterator().asScala.map(e => Triple(uid, e.getKey, e.getValue.getAsString))
  }

  def next: Boolean = triples.hasNext

  def get: InternalRow = triples.next().asInternalRow

  def close(): Unit = channels.foreach(_.shutdown())

}
