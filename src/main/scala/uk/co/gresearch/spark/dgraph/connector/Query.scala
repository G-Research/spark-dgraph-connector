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

object Query {

  def forAllProperties(resultName: String, uids: Option[UidRange]): String = {
    val pagination =
      uids.map(range => s", first: ${range.length}, offset: ${range.first}").getOrElse("")

    s"""{
       |  ${resultName} (func: has(dgraph.type)$pagination) {
       |    uid
       |    expand(_all_)
       |  }
       |}""".stripMargin
  }

  def forAllPropertiesAndEdges(resultName: String, uids: Option[UidRange]): String = {
    val pagination =
      uids.map(range => s", first: ${range.length}, offset: ${range.first}").getOrElse("")

    s"""{
       |  ${resultName} (func: has(dgraph.type)$pagination) {
       |    uid
       |    expand(_all_) {
       |      uid
       |    }
       |  }
       |}""".stripMargin
  }


  def forPropertiesAndEdges(resultName: String, predicates: Set[Predicate], uids: Option[UidRange]): String = {
      val pagination =
        uids.map(range => s", first: ${range.length}, offset: ${range.first}").getOrElse("")

      val filter =
          Option(predicates)
            .filter(_.nonEmpty)
            .map(_.map(p => s"has(${p.predicateName})").mkString(" OR "))
            // an empty predicates set must return empty result set
            .orElse(Some("eq(true, false)"))
            .map(filter => s"@filter(${filter}) ")
            .get

      val predicatesPaths =
        Option(predicates)
          .filter(_.nonEmpty)
          .map(t =>
            t.map {
              case Predicate(predicate, "uid") => s"    $predicate { uid }"
              case Predicate(predicate, _____) => s"    $predicate"
            }.mkString("\n") + "\n"
          ).getOrElse("")

      s"""{
         |  ${resultName} (func: has(dgraph.type)${pagination}) ${filter}{
         |    uid
         |${predicatesPaths}  }
         |}""".stripMargin
    }

}
