package uk.co.gresearch.spark.dgraph.connector.encoder

import java.sql.Timestamp
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import com.google.gson.{Gson, JsonArray, JsonElement, JsonObject}
import uk.co.gresearch.spark.dgraph.connector.{Geo, Json, Password, Uid}

import scala.collection.JavaConverters._

/**
 * Helper methods to turn Dgraph json results into JsonObjects representing Dgraph nodes.
 */
trait JsonNodeInternalRowEncoder extends InternalRowEncoder {

  def getNodes(json: Json, member: String): Iterator[JsonObject] = {
    new Gson().fromJson(json.string, classOf[JsonObject])
      .getAsJsonArray(member)
      .iterator()
      .asScala
      .map(_.getAsJsonObject)
  }

  def getValues(value: JsonElement): Iterable[JsonElement] = value match {
    case a: JsonArray => a.asScala
    case _ => Seq(value)
  }

  /**
   * Get the value of the given JsonElement in the given type. The value can be a JsonPrimitive
   * representing property values or a JsonObject representing an edge destination node.
   *
   * Types are interpreted as Dgraph types (where int is Long), for non-Dgraph types recognizes
   * as respective Spark / Scala types.
   *
   * @param value JsonElement
   * @param valueType type as string
   * @return typed value
   */
  def getValue(value: JsonElement, valueType: String): Any =
    valueType match {
      // https://dgraph.io/docs/query-language/#schema-types
      case "uid" => Uid(value.getAsJsonObject.get("uid").getAsString)
      case "string" => value.getAsString
      case "int" | "long" => value.getAsLong
      case "float" | "double" => value.getAsDouble
      case "datetime" | "timestamp" =>
        Timestamp.valueOf(ZonedDateTime.parse(value.getAsString, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toLocalDateTime)
      case "bool" | "boolean" => value.getAsString == "true"
      case "geo" => Geo(value.getAsString)
      case "password" => Password(value.getAsString)
      case "default" => value.getAsString
      case _ => value.getAsString
    }

  /**
   * Get the type of the given value as a string. Only supports Dgraph types (no Integer or Float),
   * returns Spark / Scala type string, not Dgraph (where int refers to Long).
   * @param value value
   * @return value's type
   */
  def getType(value: Any): String =
    value match {
      case _: Uid => "uid"
      case _: String => "string"
      case _: Long => "long"
      case _: Double => "double"
      case _: java.sql.Timestamp => "timestamp"
      case _: Boolean => "boolean"
      case _: Geo => "geo"
      case _: Password => "password"
      case _ => "default"
    }

}
