package uk.co.gresearch.spark.dgraph.connector

import com.google.gson.{Gson, JsonArray, JsonElement, JsonObject}
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
      |    expand(_all_) {
      |      uid
      |    }
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

  private def getValues(value: JsonElement): Iterable[JsonElement] = value match {
    case a: JsonArray => a.asScala
    case _ => Seq(value)
  }

  private def getTriple(s: Long, p: String, o: JsonElement): Triple = {
    val obj = o match {
      case obj: JsonObject => uidStringToLong(obj.get("uid").getAsString).toString
      case _ => o.getAsString
    }
    Triple(s, p, obj)
  }

  private def uidStringToLong(uid: String): Long =
    Some(uid)
      .filter(_.startsWith("0x"))
      .map(uid => java.lang.Long.valueOf(uid.substring(2), 16))
      .getOrElse(throw new IllegalArgumentException("DGraph uid is not a long prefixed with '0x': " + uid))

  private def toTriples(node: JsonObject): Iterator[Triple] = {
    val string = node.remove("uid").getAsString
    val uid = uidStringToLong(string)
    node.entrySet().iterator().asScala
      .flatMap(e => getValues(e.getValue).map(v => getTriple(uid, e.getKey, v)))
  }

  def next: Boolean = triples.hasNext

  def get: InternalRow = encoder.asInternalRow(triples.next())

  def close(): Unit = channels.foreach(_.shutdown())

}
