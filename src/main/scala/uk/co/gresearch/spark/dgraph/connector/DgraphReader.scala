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

import org.apache.spark.sql.{DataFrame, DataFrameReader}

case class DgraphReader(reader: DataFrameReader) {
  /**
   * Loads all triples of a Dgraph database into a DataFrame. Requires at least one target.
   * Use triples(targets.head, targets.tail: _*) if you want to provide a Seq[String].
   *
   * @param target  a target
   * @param targets more targets
   * @return triples DataFrame
   */
  def triples(target: String, targets: String*): DataFrame =
    reader
      .format(TriplesSource)
      .load(Seq(target) ++ targets: _*)

  /**
   * Loads all edges of a Dgraph database into a DataFrame. Requires at least one target.
   * Use edges(targets.head, targets.tail: _*) if want to provide a Seq[String].
   *
   * @param target  a target
   * @param targets more targets
   * @return edges DataFrame
   */
  def edges(target: String, targets: String*): DataFrame =
    reader
      .format(EdgesSource)
      .load(Seq(target) ++ targets: _*)

  /**
   * Loads all nodes of a Dgraph database into a DataFrame. Requires at least one target.
   * Use nodes(targets.head, targets.tail: _*) if you want to provide a Seq[String].
   *
   * @param target  a target
   * @param targets more targets
   * @return nodes DataFrame
   */
  def nodes(target: String, targets: String*): DataFrame =
    reader
      .format(NodesSource)
      .load(Seq(target) ++ targets: _*)
}
