/*
 * Copyright 2020 G-Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector

import java.util.UUID

import com.google.gson.{Gson, JsonObject}

import scala.collection.JavaConverters._

case class ClusterState(groupMembers: Map[String, Set[Target]],
                        groupPredicates: Map[String, Set[String]],
                        maxLeaseId: Long,
                        cid: UUID)

object ClusterState {

  def fromJson(json: Json): ClusterState = {
    val root = new Gson().fromJson(json.string, classOf[JsonObject])
    val groups = root.getAsJsonObject("groups")
    val groupMap =
      groups
        .entrySet().asScala
        .map(e => e.getKey -> e.getValue.getAsJsonObject)
        .toMap

    val groupMembers = groupMap.mapValues(getMembersFromGroup)
      .mapValues(_.map(Target).map(t => t.withPort(t.port + 2000)))
    val groupPredicates = groupMap.mapValues(getPredicatesFromGroup)
    val maxLeaseId = if (root.has("maxUID")) {
      root.getAsJsonPrimitive("maxUID").getAsLong
    } else if (root.has("maxLeaseId")) {
      root.getAsJsonPrimitive("maxLeaseId").getAsLong
    } else {
      1000L
    }
    val cid = UUID.fromString(root.getAsJsonPrimitive("cid").getAsString)

    ClusterState(groupMembers, groupPredicates, maxLeaseId, cid)
  }

  def getMembersFromGroup(group: JsonObject): Set[String] =
    Option(group.getAsJsonObject("members"))
      .map(_.entrySet().asScala)
      .getOrElse(Set.empty)
      .map(_.getValue.getAsJsonObject)
      .map(_.getAsJsonPrimitive("addr"))
      .map(_.getAsString)
      .toSet

  def getPredicatesFromGroup(group: JsonObject): Set[String] =
    Option(group.getAsJsonObject("tablets"))
      .map(_.entrySet().asScala)
      .getOrElse(Set.empty)
      .map(_.getValue.getAsJsonObject)
      .map(_.getAsJsonPrimitive("predicate").getAsString)
      .flatMap(getPredicateFromJsonString)
      .toSet

  def getPredicateFromJsonString(predicate: String): Option[String] = predicate match {
    // ≥v21.09
    case _ if predicate.startsWith("0-") => Some(predicate.replaceAll("^0-", ""))
    // =v21.03, non-default namespace predicates will be returned but won't match to the schema
    case _ if predicate.startsWith("\u0000") => Some(predicate.replaceAll("^\u0000+", ""))
    // <v21.03
    case _ if !predicate.matches("^[0-9]+-.*") => Some(predicate)
    // non-default namespace in ≥v21.09
    case _ => None
  }

}
