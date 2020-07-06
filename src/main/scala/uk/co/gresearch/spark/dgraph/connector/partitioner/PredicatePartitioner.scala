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

package uk.co.gresearch.spark.dgraph.connector.partitioner

import java.math.BigInteger
import java.security.MessageDigest

import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector._

import scala.language.implicitConversions

case class PredicatePartitioner(schema: Schema,
                                clusterState: ClusterState,
                                predicatesPerPartition: Int,
                                filters: Seq[connector.Filter] = Seq.empty)
  extends Partitioner {

  if (predicatesPerPartition <= 0)
    throw new IllegalArgumentException(s"predicatesPerPartition must be larger than zero: $predicatesPerPartition")

  def getPartitionsForPredicates(predicates: Set[_]): Int =
    if (predicates.isEmpty) 1 else 1 + (predicates.size - 1) / predicatesPerPartition

  override def supportsFilters(filters: Seq[connector.Filter]): Boolean = filters.map {
    case _: PredicateNameIsIn => true
    case _: ObjectTypeIsIn => true
    // only supported together with PredicateNameIsIn or ObjectTypeIsIn
    case _: ObjectValueIsIn => filters.exists {
      case _: PredicateNameIsIn => true
      case _: ObjectTypeIsIn => true
      case _ => false
    }
    case _ => false
  }.forall(identity)

  override def withFilters(filters: Seq[connector.Filter]): Partitioner = copy(filters = filters)

  override def getPartitions: Seq[Partition] = {
    val processedFilters = replaceObjectTypeIsInFilter(filters)
    // TODO: interset filters again
    val cState = filter(clusterState, processedFilters)
    val values = getValues(processedFilters)
    val partitionsPerGroup = cState.groupPredicates.mapValues(getPartitionsForPredicates)
    PredicatePartitioner.getPartitions(schema, cState, partitionsPerGroup, values)
  }

  /**
   * Replaces ObjectTypeIsIn filter with PredicateNameIsIn filter having all predicate names
   * of the respective type. Only with PredicateNameIsIn we can apply ObjectValueIsIn filters.
   * @param filters filters
   * @return filters with ObjectTypeIsIn replaced
   */
  def replaceObjectTypeIsInFilter(filters: Seq[Filter]): Seq[Filter] =
    filters.map {
      case ObjectTypeIsIn(types) =>
        val predicateNames = schema.predicates.filter(p => types.contains(p.typeName)).map(_.predicateName)
        PredicateNameIsIn(predicateNames)
      case f: Filter => f
    }

  def filter(clusterState: ClusterState, filters: Seq[connector.Filter]): ClusterState =
    filters.foldLeft(clusterState)(filter)

  def filter(clusterState: ClusterState, filter: connector.Filter): ClusterState =
    filter match {
      case f: PredicateNameIsIn => clusterState.copy(
        groupPredicates = clusterState.groupPredicates.mapValues(_.filter(f.names))
      )
      case f: ObjectTypeIsIn =>
        throw new IllegalArgumentException("there should be no ObjectTypeIsIn in filters, use replaceObjectTypeIsInFilter")
      case _ => clusterState
    }

  def getValues(filters: Seq[Filter]): Map[String, Set[Any]] = {
    val predicates = filters.flatMap {
      case PredicateNameIsIn(predicateNames) => Some(predicateNames)
      case _ => None
    }.headOption.getOrElse(Set.empty)

    val values = filters.flatMap {
      case ObjectValueIsIn(values) => Some(values)
      case _ => None
    }.headOption.getOrElse(Set.empty)

    predicates.map(predicate => predicate -> values).toMap.filter(_._2.nonEmpty)
  }

}

object PredicatePartitioner extends ClusterStateHelper {

  val md5: MessageDigest = MessageDigest.getInstance("MD5")

  /**
   * Compute MD5 hash of predicate name. Hash is a BigInt.
   * @param predicate predicate
   * @return BigInt hash
   */
  def hash(predicate: Predicate): BigInt = {
    val digest = md5.digest(predicate.predicateName.getBytes)
    new BigInteger(1,digest)
  }

  /**
   * Shards a set of predicates based on the MD5 hash. Shards are probably even-sized,
   * but this is not guaranteed.
   * @param predicates set of predicates
   * @param shards number of shards
   * @return predicates shard
   */
  def shard(predicates: Set[Predicate], shards: Int): Seq[Set[Predicate]] = {
    if (shards < 1) throw new IllegalArgumentException(s"shards must be larger than zero: $shards")
    predicates.groupBy(hash(_) % shards).values.toSeq
  }

  /**
   * Partitions a set of predicates in equi-sized partitions. Predicates get sorted by MD5 hash and
   * then round-robin assigned to partitions.
   * @param predicates set of predicates
   * @param partitions number of partitions
   * @return partitions
   */
  def partition(predicates: Set[Predicate], partitions: Int): Seq[Set[Predicate]] = {
    if (partitions < 1)
      throw new IllegalArgumentException(s"partitions must be larger than zero: $partitions")

    predicates
      // turn into seq and sort by hash (consistently randomize)
      .toSeq.sortBy(hash)
      // add index to predicates
      .zipWithIndex
      // group by predicate index % partitions
      .groupBy(_._2 % partitions)
      // sort by partition id
      .toSeq.sortBy(_._1)
      // drop keys and remove index from (predicate, index) tuple, restore set
      .map(_._2.map(_._1).toSet)
  }

  def getPartitions(schema: Schema,
                    clusterState: ClusterState,
                    partitionsInGroup: (String) => Int,
                    values: Map[String, Set[Any]]): Seq[Partition] =
    clusterState.groupPredicates.keys.flatMap { group =>
      val targets = getGroupTargets(clusterState, group).toSeq.sortBy(_.target)
      val partitions = partitionsInGroup(group)
      val groupPredicates = getGroupPredicates(clusterState, group, schema)
      val groupPredicateNames = groupPredicates.map(_.predicateName)
      val predicatesPartitions = partition(groupPredicates, partitions)
      val groupValues = Some(values.filterKeys(groupPredicateNames.contains)).filter(_.nonEmpty)

      predicatesPartitions.indices.map { index =>
        Partition(targets.rotateLeft(index), Some(predicatesPartitions(index)), None, groupValues)
      }
    }.toSeq

}
