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

package uk.co.gresearch.spark.dgraph.connector.partitioner

import java.math.BigInteger
import java.security.MessageDigest

import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector._

import scala.language.implicitConversions

case class PredicatePartitioner(schema: Schema,
                                clusterState: ClusterState,
                                predicatesPerPartition: Int,
                                filters: Filters = EmptyFilters,
                                projection: Option[Seq[Predicate]] = None)
  extends Partitioner with Logging {

  if (predicatesPerPartition <= 0)
    throw new IllegalArgumentException(s"predicatesPerPartition must be larger than zero: $predicatesPerPartition")

  def getPartitionsForPredicates(predicates: Set[_]): Int =
    if (predicates.isEmpty) 1 else 1 + (predicates.size - 1) / predicatesPerPartition

  val props: Set[String] = schema.predicates.filter(_.isProperty).map(_.predicateName)
  val edges: Set[String] = schema.predicates.filter(_.isEdge).map(_.predicateName)

  override def supportsFilters(filters: Set[connector.Filter]): Boolean = filters.map {
    case _: AlwaysFalse => true
    case _: SubjectIsIn => true
    case _: PredicateNameIsIn => true
    case _: PredicateValueIsIn => true
    case _: ObjectTypeIsIn => true
    // only supported together with PredicateNameIsIn or ObjectTypeIsIn
    case _: ObjectValueIsIn => filters.exists {
      case _: PredicateNameIsIn => true
      case _: ObjectTypeIsIn => true
      case _ => false
    }
    case _ => false
  }.forall(identity)

  override def withFilters(filters: Filters): Partitioner = copy(filters = filters)

  override def withProjection(projection: Seq[Predicate]): Partitioner = copy(projection = Some(projection))

  override def getPartitions: Seq[Partition] = {
    val processedFilters = replaceObjectTypeIsInFilter(filters)
    val simplifiedFilters = FilterTranslator.simplify(processedFilters, supportsFilters)
    val cState = filter(clusterState, simplifiedFilters)
    val partitionsPerGroup = cState.groupPredicates.mapValues(getPartitionsForPredicates)

    log.trace(s"replaced filters: $processedFilters")
    log.trace(s"simplified filters: $simplifiedFilters")

    PredicatePartitioner.getPartitions(schema, cState, partitionsPerGroup, simplifiedFilters, projection)
  }

  /**
   * Replaces ObjectTypeIsIn filter in required and optional filters
   * using replaceObjectTypeIsInFilter(filters: Seq[Filter]).
   *
   * @param filters filters
   * @return filters with ObjectTypeIsIn replaced
   */
  def replaceObjectTypeIsInFilter(filters: Filters): Filters =
    Filters(
      replaceObjectTypeIsInFilter(filters.promised),
      replaceObjectTypeIsInFilter(filters.optional)
    )

  /**
   * Replaces ObjectTypeIsIn filter with IntersectPredicateNameIsIn filter having all predicate names
   * of the respective type. Only with (Intersect)PredicateNameIsIn we can apply ObjectValueIsIn filters.
   *
   * @param filters filters
   * @return filters with ObjectTypeIsIn replaced
   */
  def replaceObjectTypeIsInFilter(filters: Set[Filter]): Set[Filter] =
    filters.map {
      case ObjectTypeIsIn(types) =>
        val predicateNames = schema.predicates.filter(p => types.contains(p.sparkType)).map(_.predicateName)
        IntersectPredicateNameIsIn(predicateNames)
      case f: Filter => f
    }

  def filter(clusterState: ClusterState, filters: Set[connector.Filter]): ClusterState =
    filters.foldLeft(clusterState)(filter)

  def filter(clusterState: ClusterState, filter: connector.Filter): ClusterState =
    filter match {
      case _: AlwaysFalse => clusterState.copy(groupPredicates = Map.empty)
      // only intersect PredicateNameIsIn limits the predicates for Has and Get
      case f: IntersectPredicateNameIsIn => clusterState.copy(
        groupPredicates = clusterState.groupPredicates.mapValues(_.filter(f.names))
      )
      // only intersect PredicateValueIsIn limits the predicates for Has and Get
      case f: IntersectPredicateValueIsIn => clusterState.copy(
        groupPredicates = clusterState.groupPredicates.mapValues(_.filter(f.names))
      )
      case _: ObjectTypeIsIn =>
        throw new IllegalArgumentException("any ObjectTypeIsIn filter should have been replaced in replaceObjectTypeIsInFilter")
      case _ => clusterState
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

  /**
   * Provides operators that implement the given filters (if supported) for the given predicates.
   * @param filters filters
   * @param predicates predicates to filter
   * @param properties names of properties
   * @param edges names of edges
   * @return operators
   */
  def getFilterOperators(filters: Set[Filter], predicates: Set[Predicate], properties: Set[String], edges: Set[String]): Set[Operator] = {
    val predicateNames = predicates.map(_.predicateName)
    val ops =
      filters.flatMap {
        case SubjectIsIn(uids) =>
          Seq(Uids(uids))
        case f: PredicateNameIsIn =>
          val predNames = f.names.intersect(predicateNames)
          Seq(Has(predNames.intersect(properties), predNames.intersect(edges)))
        case f: PredicateValueIsIn =>
          val predNames = f.names.intersect(predicateNames)
          Seq(Has(predNames.intersect(properties), predNames.intersect(edges)), IsIn(f.names, f.values))
        case _ => Seq.empty
      }
        .map(_.asInstanceOf[Operator])

    if (!ops.exists(_.isInstanceOf[Has])) {
      Set[Operator](Has(predicateNames.intersect(properties), predicateNames.intersect(edges))) ++ ops
    } else {
      ops
    }
  }

  def getLangDirectives(schema: Schema, partitions: Seq[Set[Predicate]]): Seq[Set[String]] = {
    val langPredicates = schema.predicates.filter(_.isLang).map(_.predicateName)
    partitions.map(_.map(_.predicateName).intersect(langPredicates))
  }

  def getPartitions(schema: Schema,
                    clusterState: ClusterState,
                    partitionsInGroup: (String) => Int,
                    filters: Set[Filter],
                    projection: Option[Seq[Predicate]]): Seq[Partition] =
    clusterState.groupPredicates.keys.flatMap { group =>
      val targets = getGroupTargets(clusterState, group).toSeq.sortBy(_.target)
      val partitions = partitionsInGroup(group)
      val groupPredicates = getGroupPredicates(clusterState, group, schema)
      val predicatesPartitions = partition(groupPredicates, partitions)
      val langPartitions = getLangDirectives(schema, predicatesPartitions)
      val (props, edges) = schema.predicates.partition(_.isProperty)
      val propNames = props.map(_.predicateName)
      val edgeNames = edges.map(_.predicateName)

      predicatesPartitions.indices.map { index =>
        Partition(
          targets.rotateLeft(index),
          getFilterOperators(filters, predicatesPartitions(index), propNames, edgeNames)
        )
          .langs(langPartitions(index))
          .get(projection.foldLeft(predicatesPartitions(index))((preds, proj) => preds.intersect(proj.toSet)))
      }
    }.toSeq

}
