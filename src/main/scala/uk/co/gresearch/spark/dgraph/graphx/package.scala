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

import java.sql.Timestamp

import org.apache.spark.graphx.{Graph, VertexId, Edge => GraphxEdge}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameReader, Encoder, Encoders, SparkSession}
import uk.co.gresearch.spark.dgraph.connector._

package object graphx extends TargetsConfigParser {

  class VertexProperty(val property: String, val value: Any) extends Serializable
  case class StringVertexProperty(override val property: String, override val value: String) extends VertexProperty(property, value)
  case class LongVertexProperty(override val property: String, override val value: Long) extends VertexProperty(property, value)
  case class DoubleVertexProperty(override val property: String, override val value: Double) extends VertexProperty(property, value)
  case class TimestampVertexProperty(override val property: String, override val value: Timestamp) extends VertexProperty(property, value)
  case class BooleanVertexProperty(override val property: String, override val value: Boolean) extends VertexProperty(property, value)
  case class GeoVertexProperty(override val property: String, override val value: Geo) extends VertexProperty(property, value)
  case class PasswordVertexProperty(override val property: String, override val value: Password) extends VertexProperty(property, value)

  case class EdgeProperty(property: String)

  val edgeEncoder: Encoder[Edge] = Encoders.product[Edge]
  val nodeEncoder: Encoder[TypedNode] = Encoders.product[TypedNode]

  def loadGraph(targets: String*)(implicit session: SparkSession): Graph[VertexProperty, EdgeProperty] =
    loadGraph(session.read, targets.map(Target): _*)

  def loadGraph(reader: DataFrameReader, targets: Target*): Graph[VertexProperty, EdgeProperty] =
    Graph(loadVertices(reader, targets: _*), loadEdges(reader, targets: _*))

  def loadVertices(targets: String*)(implicit session: SparkSession): RDD[(VertexId, VertexProperty)] =
    loadVertices(session.read, targets.map(Target): _*)

  def loadVertices(reader: DataFrameReader, targets: Target*): RDD[(VertexId, VertexProperty)] =
    reader
      .format(NodesSource)
      .load(targets.map(_.target): _*)
      .as[TypedNode](nodeEncoder)
      .rdd
      .map(toVertex)

  def loadEdges(targets: String*)(implicit session: SparkSession): RDD[GraphxEdge[EdgeProperty]] =
    loadEdges(session.read, targets.map(Target): _*)

  def loadEdges(reader: DataFrameReader, targets: Target*): RDD[GraphxEdge[EdgeProperty]] =
    reader
      .format(EdgesSource)
      .load(targets.map(_.target): _*)
      .as[Edge](edgeEncoder)
      .rdd
      .map(toEdge)

  def toVertex(row: TypedNode): (VertexId, VertexProperty) =
    (row.subject, toVertexProperty(row))

  def toEdge(row: Edge): GraphxEdge[EdgeProperty] =
    GraphxEdge(row.subject, row.objectUid, EdgeProperty(row.predicate))
  
  def toVertexProperty(row: TypedNode): VertexProperty = row.objectType match {
    case "string" => row.objectString.map(StringVertexProperty(row.predicate, _)).orNull
    case "long" => row.objectLong.map(LongVertexProperty(row.predicate, _)).orNull
    case "double" => row.objectDouble.map(DoubleVertexProperty(row.predicate, _)).orNull
    case "timestamp" => row.objectTimestamp.map(TimestampVertexProperty(row.predicate, _)).orNull
    case "boolean" => row.objectBoolean.map(BooleanVertexProperty(row.predicate, _)).orNull
    case "geo" => row.objectGeo.map(o => GeoVertexProperty(row.predicate, Geo(o))).orNull
    case "password" => row.objectPassword.map(o => PasswordVertexProperty(row.predicate, Password(o))).orNull
    case "default" => row.objectString.map(StringVertexProperty(row.predicate, _)).orNull
    case _ =>
      throw new IllegalArgumentException(s"Unsupported object type ${row.objectType} in node row: $row")
  }

  implicit class DgraphDataFrameReader(reader: DataFrameReader) {

    def dgraph(targets: String*): Graph[VertexProperty, EdgeProperty] =
      graphx.loadGraph(reader, targets.map(Target): _*)

    def dgraphVertices(targets: String*): RDD[(VertexId, VertexProperty)] =
      graphx.loadVertices(reader, targets.map(Target): _*)

    def dgraphEdges(targets: String*): RDD[GraphxEdge[EdgeProperty]] =
      graphx.loadEdges(reader, targets.map(Target): _*)

  }

}
