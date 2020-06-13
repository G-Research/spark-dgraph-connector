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

import java.util.UUID

import com.google.gson.{Gson, JsonObject}

import scala.collection.JavaConverters._

case class ClusterState(groupMembers: Map[String, Set[Target]],
                        groupPredicates: Map[String, Set[String]],
                        maxLeaseId: Long,
                        cid: UUID)

object ClusterState {

  def fromJson(json: String): ClusterState = {
    val root = new Gson().fromJson(json, classOf[JsonObject])
    val groups = root.getAsJsonObject("groups")
    val groupMap =
      groups
        .entrySet().asScala
        .map(e => e.getKey -> e.getValue.getAsJsonObject)
        .toMap

    val groupMembers = groupMap.mapValues(getMembersFromGroup)
      .mapValues(_.map(Target).map(t => t.withPort(t.port + 2000)))
    val groupPredicates = groupMap.mapValues(getPredicatesFromGroup)
    val maxLeaseId = root.getAsJsonPrimitive("maxLeaseId").getAsLong
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
      .map(_.getAsJsonPrimitive("predicate"))
      .map(_.getAsString)
      .toSet

}
