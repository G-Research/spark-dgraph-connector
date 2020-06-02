package uk.co.gresearch.spark.dgraph.connector

import com.google.gson.{Gson, JsonArray, JsonElement, JsonObject}

import scala.collection.JavaConverters._

object TriplesFactory {

  def fromJson(json: String): Iterator[Triple] = {
    new Gson().fromJson(json, classOf[JsonObject])
      .getAsJsonArray("nodes")
      .iterator()
      .asScala
      .map(_.getAsJsonObject)
      .flatMap(toTriples)
  }

  private def toTriples(node: JsonObject): Iterator[Triple] = {
    val string = node.remove("uid").getAsString
    val uid = uidStringToLong(string)
    node.entrySet().iterator().asScala
      .flatMap(e => getValues(e.getValue).map(v => getTriple(uid, e.getKey, v)))
  }

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

}
