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

import com.google.gson.{Gson, JsonObject, JsonPrimitive}

import java.util.UUID
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class ClusterState(groupMembers: Map[String, Set[Target]],
                        groupPredicates: Map[String, Set[String]],
                        maxLeaseId: Option[BigInt],
                        cid: UUID)

object ClusterState extends Logging {

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
    val cid = UUID.fromString(root.getAsJsonPrimitive("cid").getAsString)

    val maxLeaseId = (if (root.has("maxUID")) {
      getBigIntFromJson(root.getAsJsonPrimitive("maxUID"))
    } else if (root.has("maxLeaseId")) {
      getBigIntFromJson(root.getAsJsonPrimitive("maxLeaseId"))
    } else {
      Failure(new IllegalArgumentException("Cluster state does not contain maxLeaseId or maxUID, this disables uid range partitioning"))
    }) match {
      case Success(value) => Some(value)
      case Failure(exception) =>
        log.error("Failed to retrieve maxLeaseId from cluster state", exception)
        None
    }

    if (maxLeaseId.exists(_ < 0)) {
      log.error(s"Cluster state indicates negative maxLeaseId or maxUID, this disables uid range partitioning: ${maxLeaseId.get}")
    }
    val positiveMaxLeaseId = maxLeaseId.filter(_ >= 0)

    ClusterState(groupMembers, groupPredicates, positiveMaxLeaseId, cid)
  }

  def getBigIntFromJson(n: JsonPrimitive): Try[BigInt] = {
    Try(n.getAsBigInteger).map(BigInt.javaBigInteger2bigInt)
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
