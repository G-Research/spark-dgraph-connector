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

package uk.co.gresearch.spark.dgraph

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.graphframes.GraphFrame
import uk.co.gresearch.spark.dgraph.connector._

package object graphframes {

  def loadGraph(targets: String*)(implicit session: SparkSession): GraphFrame =
    loadGraph(session.read, targets: _*)

  def loadGraph(reader: DataFrameReader, targets: String*): GraphFrame =
    GraphFrame(loadVertices(reader, targets: _*), loadEdges(reader, targets: _*))

  def loadVertices(targets: String*)(implicit session: SparkSession): DataFrame =
    loadVertices(session.read, targets: _*)

  def loadVertices(reader: DataFrameReader, targets: String*): DataFrame = {
    val vertices =
      DgraphDataFrameReader(
        reader.option(NodesModeOption, NodesModeWideOption)
      )
        .dgraph.nodes(targets.head, targets.tail: _*)
        .withColumnRenamed("subject", "id")
    val renamedColumns =
      vertices.columns.map(f =>
        col(s"`${f}`").as(f.replace("_", "__").replace(".", "_"))
      )
    vertices.select(renamedColumns: _*)
  }

  def loadEdges(targets: String*)(implicit session: SparkSession): DataFrame =
    loadEdges(session.read, targets: _*)

  def loadEdges(reader: DataFrameReader, targets: String*): DataFrame =
    DgraphDataFrameReader(reader)
      .dgraph.edges(targets.head, targets.tail: _*)
      .select(
        col("subject").as("src"),
        col("objectUid").as("dst"),
        col("predicate")
      )

  implicit class GraphFrameDataFrameReader(reader: DataFrameReader) {
    def dgraph: GraphFramesReader = GraphFramesReader(reader)
  }

}
