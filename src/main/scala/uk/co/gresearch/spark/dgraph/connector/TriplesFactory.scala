/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector

import java.sql.Timestamp
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import com.google.gson.{Gson, JsonArray, JsonElement, JsonObject}

import scala.collection.JavaConverters._

case class TriplesFactory(schema: Schema) {

  def fromJson(json: String, member: String): Iterator[Triple] = {
    new Gson().fromJson(json, classOf[JsonObject])
      .getAsJsonArray(member)
      .iterator()
      .asScala
      .map(_.getAsJsonObject)
      .flatMap(toTriples)
  }

  def toTriples(node: JsonObject): Iterator[Triple] = {
    val uid = node.remove("uid").getAsString
    node.entrySet().iterator().asScala
      .flatMap(e => getValues(e.getValue).map(v => getTriple(uid, e.getKey, v)))
  }

  def getValues(value: JsonElement): Iterable[JsonElement] = value match {
    case a: JsonArray => a.asScala
    case _ => Seq(value)
  }

  /**
   * Get the value of the given JsonElement in the given type.
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
      case "string" => value.getAsString
      case "int" | "long" => value.getAsLong
      case "float" | "double" => value.getAsDouble
      case "datetime" | "timestamp" =>
        Timestamp.valueOf(ZonedDateTime.parse(value.getAsString, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toLocalDateTime)
      case "bool" | "boolean" => value.getAsString == "true"
      case "uid" => Uid(value.getAsString)
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

  def getTriple(s: String, p: String, o: JsonElement): Triple = {
    val uid = Uid(s)
    val obj = o match {
      case obj: JsonObject => Uid(obj.get("uid").getAsString)
      case _ => getValue(o, schema.predicateMap.get(p).map(_.typeName).getOrElse("unknown"))
    }
    Triple(uid, p, obj)
  }

}
