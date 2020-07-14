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

case class PartitionQuery(resultName: String,
                          predicates: Set[Predicate],
                          values: Option[Map[String, Set[Any]]]) {

  def getChunkString(chunk: Option[Chunk]): String =
    chunk.map(c => s", first: ${c.length}, after: ${c.after.toHexString}").getOrElse("")

  def getValueFilter(predicateName: String, filterMode: String): String = {
    predicates
      .find(_.predicateName.equals(predicateName))
      .flatMap(predicateType =>
        values
          .flatMap(_.get(predicateName))
          .map { valueSet =>
            val filter = predicateType match {
              case Predicate(_, "uid", _) if filterMode.equals("vals") =>
                s"""uid(${valueSet.map(Uid(_).toHexString).mkString(", ")})"""
              case _ => valueSet.map { value =>
                predicateType match {
                  case Predicate(_, "uid", _) => filterMode match {
                    // not needed
                    // case "vals" => s"""uid(${Uid(value).toHexString})"""
                    case "uids" => s"""uid_in(<$predicateName>, ${Uid(value).toHexString})"""
                    case _ => throw new IllegalArgumentException(s"unsupported filter mode: $filterMode")
                  }
                  case ______________________ => s"""eq(<$predicateName>, "${value.toString}")"""
                }
              }.mkString(" OR ")
            }

            Some(filter)
              .filter(_.nonEmpty)
              .map(f => s" @filter($f)")
              .getOrElse("")
          }
      )
      .getOrElse("")
  }

  def getPredicateQueries(chunk: Option[Chunk]): Map[String, String] =
    predicates
      .zipWithIndex
      .map { case (pred, idx) => s"pred${idx+1}" -> s"""pred${idx+1} as var(func: has(<${pred.predicateName}>)${getChunkString(chunk)})${getValueFilter(pred.predicateName, "uids")}""" }
      .toMap

  val predicatePaths: Seq[String] =
    predicates
      .map {
        case Predicate(predicate, "uid", _) => s"<$predicate> { uid }${getValueFilter(predicate, "vals")}"
        case Predicate(predicate, _____, _) => s"<$predicate>${getValueFilter(predicate, "vals")}"
      }
      .toSeq

  /**
   * Provides the GraphQl query for the given chunk, or if no chunk is given the query for the entire
   * result set.
   * @param chunk optional chunk
   * @return result set or chunk of it
   */
  def forChunk(chunk: Option[connector.Chunk]): GraphQl = {
    val predicateQueries = getPredicateQueries(chunk)
    val query =
      s"""{${predicateQueries.values.map(query => s"\n  $query").mkString}${if(predicateQueries.nonEmpty) "\n" else ""}
         |  ${resultName} (func: uid(${predicateQueries.keys.mkString(",")})${getChunkString(chunk)}) {
         |    uid
         |${predicatePaths.map(path => s"    $path\n").mkString}  }
         |}""".stripMargin

    GraphQl(query)
  }

}

object PartitionQuery {
  def of(partition: Partition, resultName: String = "result"): PartitionQuery =
    PartitionQuery(resultName, partition.predicates, partition.values)
}
