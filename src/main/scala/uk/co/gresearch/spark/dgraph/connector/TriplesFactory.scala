package uk.co.gresearch.spark.dgraph.connector

import java.sql.Timestamp
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import com.google.gson.{Gson, JsonArray, JsonElement, JsonObject}

import scala.collection.JavaConverters._

private object TriplesFactory {

  def fromJson(json: String, member: String, predicates: Option[Set[Predicate]] = None): Iterator[Triple] = {
    val predicateMap = predicates.map(_.map(p => p.predicateName -> p).toMap)
    new Gson().fromJson(json, classOf[JsonObject])
      .getAsJsonArray(member)
      .iterator()
      .asScala
      .map(_.getAsJsonObject)
      .flatMap(toTriples(predicateMap))
  }

  def toTriples(predicates: Option[Map[String, Predicate]])(node: JsonObject): Iterator[Triple] = {
    val uid = node.remove("uid").getAsString
    node.entrySet().iterator().asScala
      .flatMap(e => getValues(e.getValue).map(v => getTriple(predicates)(uid, e.getKey, v)))
  }

  def getValues(value: JsonElement): Iterable[JsonElement] = value match {
    case a: JsonArray => a.asScala
    case _ => Seq(value)
  }

  /**
   * Get the value of the given JsonElement in the given optional type.
   * Types are interpreted as Dgraph types (where int is Long), for non-Dgraph types recognizes
   * as respective Spark / Scala types.
   *
   * @param value JsonElement
   * @param valueType optional type as string
   * @return typed value
   */
  def getValue(value: JsonElement, valueType: Option[String]): Any =
    valueType match {
      // https://dgraph.io/docs/query-language/#schema-types
      case Some("string") => value.getAsString
      case Some("int") | Some("long") => value.getAsLong
      case Some("float") | Some("double") => value.getAsDouble
      case Some("datetime") | Some("timestamp") =>
        Timestamp.valueOf(ZonedDateTime.parse(value.getAsString, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toLocalDateTime)
      case Some("bool") | Some("boolean") => value.getAsString == "true"
      case Some("uid") => Uid(value.getAsString)
      case Some("geo") => Geo(value.getAsString)
      case Some("password") => Password(value.getAsString)
      case Some("default") => value.getAsString
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
      case _: String => "string"
      case _: Long => "long"
      case _: Double => "double"
      case _: java.sql.Timestamp => "timestamp"
      case _: Boolean => "boolean"
      case _: Uid => "uid"
      case _: Geo => "geo"
      case _: Password => "password"
      case _ => "default"
    }

  def getTriple(predicates: Option[Map[String, Predicate]])(s: String, p: String, o: JsonElement): Triple = {
    val uid = Uid(s)
    val obj = o match {
      case obj: JsonObject => Uid(obj.get("uid").getAsString)
      case _ => getValue(o, predicates.flatMap(_.get(p)).map(_.typeName))
    }
    Triple(uid, p, obj)
  }

}
