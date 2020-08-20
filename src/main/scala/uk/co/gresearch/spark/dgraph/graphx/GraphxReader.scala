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

package uk.co.gresearch.spark.dgraph.graphx

import org.apache.spark.graphx.{Graph, VertexId, Edge => GraphxEdge}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrameReader
import uk.co.gresearch.spark.dgraph
import uk.co.gresearch.spark.dgraph.connector.Target

case class GraphxReader(reader: DataFrameReader) {
  def graphx(targets: String*): Graph[VertexProperty, EdgeProperty] =
    dgraph.graphx.loadGraph(reader, targets.map(Target): _*)

  def vertices(targets: String*): RDD[(VertexId, VertexProperty)] =
    dgraph.graphx.loadVertices(reader, targets.map(Target): _*)

  def edges(targets: String*): RDD[GraphxEdge[EdgeProperty]] =
    dgraph.graphx.loadEdges(reader, targets.map(Target): _*)
}
