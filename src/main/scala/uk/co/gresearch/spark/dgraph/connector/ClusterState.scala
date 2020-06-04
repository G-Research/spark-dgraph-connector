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

    val groupMembers = groupMap.mapValues(getMembersFromGroup).mapValues(_.map(Target))
    val groupPredicates = groupMap.mapValues(getPredicatesFromGroup)
    val maxLeaseId = root.getAsJsonPrimitive("maxLeaseId").getAsLong
    val cid = UUID.fromString(root.getAsJsonPrimitive("cid").getAsString)

    ClusterState(groupMembers, groupPredicates, maxLeaseId, cid)
  }

  def getMembersFromGroup(group: JsonObject): Set[String] =
    group
      .getAsJsonObject("members")
      .entrySet().asScala
      .map(_.getValue.getAsJsonObject)
      .map(_.getAsJsonPrimitive("addr"))
      .map(_.getAsString)
      .toSet

  def getPredicatesFromGroup(group: JsonObject): Set[String] =
    group
      .getAsJsonObject("tablets")
      .entrySet().asScala
      .map(_.getValue.getAsJsonObject)
      .map(_.getAsJsonPrimitive("predicate"))
      .map(_.getAsString)
      .toSet

}
