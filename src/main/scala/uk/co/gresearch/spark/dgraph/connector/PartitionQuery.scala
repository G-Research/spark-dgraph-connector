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

import uk.co.gresearch.spark.dgraph.connector

/**
 * Defines the query for a specific partition. Operators define the set of result uids and
 * predicates to receive and are all mandatory. If no Get operators are given,
 * predicates for all Has operators are retrieved.
 *
 * Operators are evaluated as AND.
 *
 * @param resultName result name in the JSON query
 * @param operators set of operators
 */
case class PartitionQuery(resultName: String, operators: Set[Operator]) {

  val uidsOps: Set[Uid] =
    operators
      .filter(_.isInstanceOf[Uids])
      .flatMap { case Uids(uids) => uids }

  val hasPredicates: Set[Set[String]] =
    operators
      .filter(_.isInstanceOf[Has])
      .map { case Has(properties, edges) => properties ++ edges }

  val predicateVals: Map[String, String] =
    hasPredicates
      .flatten
      .toSeq.sorted
      .zipWithIndex
      .map { case (predicate, idx) => predicate -> s"pred${idx + 1}" }
      .toMap

  val (hasProperties, hasEdges) =
    operators
      .filter(_.isInstanceOf[Has])
      .map { case Has(properties, edges) => (properties, edges) }
      .fold((Set.empty[String], Set.empty[String])) { case ((leftP, leftE), (rightP, rightE)) => (leftP ++ rightP, leftE ++ rightE) }

  val (getProperties, getEdges) =
    Some(operators
      .filter(_.isInstanceOf[Get])
      .map { case Get(properties, edges) => (properties, edges) }
    ).filter(_.nonEmpty)
      .getOrElse(operators.filter(_.isInstanceOf[Has]).map { case Has(properties, edges) => (properties, edges) })
      .fold((Set.empty[String], Set.empty[String])) { case ((leftP, leftE), (rightP, rightE)) => (leftP ++ rightP, leftE ++ rightE) }

  val properties: Set[String] = (hasProperties ++ getProperties)
  val edges: Set[String] = (hasEdges ++ getEdges)

  val predicateOps: Map[String, Set[PredicateOperator]] =
    operators
      .filter(op => op.isInstanceOf[PredicateOperator])
      .map(op => op.asInstanceOf[PredicateOperator])
      .flatMap(op => op.predicates.map(predicate => predicate -> op))
      .groupBy(_._1)
      .mapValues(_.map(_._2))

  /**
   * Provides the GraphQl query for the given chunk, or if no chunk is given
   * the query for the entire result set.
   *
   * @param chunk optional chunk
   * @return result set or chunk of it
   */
  def forChunk(chunk: Option[connector.Chunk]): GraphQl = {
    val predicateQueries = getPredicateQueries(chunk)
    val predicateQueriesValues =
      Some(predicateQueries.toSeq.sortBy(_._1).map(_._2).map(query => s"\n  $query").mkString)
        .filter(_.nonEmpty).map(f => s"$f\n")
        .getOrElse("")
    val predicateQueriesValuesWhenNoUids =
      Some(uidsOps)
        .filter(_.isEmpty).map(_ => predicateQueriesValues)
        .getOrElse("")
    val uids =
      Some(uidsOps)
        .filter(_.nonEmpty).map(_.map(_.toHexString)).map(_.mkString(","))
        .getOrElse(predicateQueries.keys.toSeq.sorted.mkString(","))
    val resultOperatorFilter = resultOperatorFilters.map(f => s"@filter($f) ").getOrElse("")
    val query =
      s"""{$predicateQueriesValuesWhenNoUids
         |  ${resultName} (func: uid($uids)${getChunkString(chunk)}) $resultOperatorFilter{
         |    uid
         |${predicatePaths.map(path => s"    $path\n").mkString}  }
         |}""".stripMargin

    GraphQl(query)
  }

  def getChunkString(chunk: Option[Chunk]): String =
    chunk.map(c => s", first: ${c.length}, after: ${c.after.toHexString}").getOrElse("")

  def getValueFilter(predicateName: String, filterMode: String): String =
    predicateOps
      .get(predicateName)
      .map(ops => ops.map(getFilter(_, predicateName, filterMode, ops.size > 1)).mkString(" AND "))
      .filter(_.nonEmpty)
      .map(f => s" @filter($f)")
      .getOrElse("")

  def getFilter(operator: Operator, predicateName: String, filterMode: String, andMode: Boolean): String = operator match {
    // we assume operator's predicates contain predicateName
    case IsIn(_, values) if edges.contains(predicateName) && filterMode.eq("vals") =>
      s"""uid(${values.map(Uid(_).toHexString).mkString(", ")})"""
    case IsIn(_, values) if edges.contains(predicateName) =>
      val filter = values.map(value => s"""uid_in(<$predicateName>, ${Uid(value).toHexString})""").mkString(" OR ")
      if (andMode && values.size > 1) s"($filter)" else filter
    // includes IsIn for properties
    case op: PredicateValuesOperator =>
      val filter = op.values.map(value => s"""${op.filter}(<$predicateName>, "${value}")""").mkString(" OR ")
      if (andMode && op.values.size > 1) s"($filter)" else filter
    case op: PredicateValueOperator => s"""${op.filter}(<$predicateName>, "${op.value}")"""
  }

  def getPredicateQueries(chunk: Option[Chunk]): Map[String, String] =
    hasPredicates
      .flatten
      .map(pred =>
        predicateVals(pred) -> s"""${predicateVals(pred)} as var(func: has(<$pred>)${getChunkString(chunk)})${getValueFilter(pred, "uids")}"""
      )
      .toMap

  val predicatePaths: Seq[String] =
    (getProperties.map(pred => pred -> s"<$pred>") ++ getEdges.map(edge => edge -> s"<$edge> { uid }"))
      .toSeq.sortBy(_._1)
      .map { case (pred, path) => s"$path${getValueFilter(pred, "vals")}" }

  val resultOperatorFilters: Option[String] = {
    val filters: Seq[String] =
      hasPredicates
        .map(_.toSeq.sorted)
        .toSeq.sortBy(_.headOption)
        .map { preds =>
          val filter = preds.flatMap {
            case pred if predicateOps.contains(pred) =>
              val ops = predicateOps(pred)
              ops.map(op => getFilter(op, pred, "uids", hasPredicates.size > 1))
            case pred => Seq(s"has(<$pred>)")
          }.mkString(" OR ")
          if (preds.size > 1 && hasPredicates.size > 1) s"($filter)" else filter
        }
    Some(filters.mkString(" AND "))
      // we only need result operator filter when there are multiple has operators
      .filter(_ => hasPredicates.size > 1)
      // we only want to add a trailing space if our filter string is non-empty
      .filter(_.nonEmpty)
  }

}

object PartitionQuery {
  def of(partition: Partition, resultName: String = "result"): PartitionQuery =
    PartitionQuery(resultName, partition.operators)
}
