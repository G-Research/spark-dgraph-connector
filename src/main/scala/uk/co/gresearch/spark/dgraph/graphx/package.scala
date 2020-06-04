package uk.co.gresearch.spark.dgraph

import java.sql.Timestamp

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameReader, Dataset, Encoder, Encoders, SparkSession}
import uk.co.gresearch.spark.dgraph.connector._

package object graphx extends TargetsConfigParser {

  class VertexProperty(property: String, value: Any) extends Serializable
  case class StringVertexProperty(property: String, value: String) extends VertexProperty(property, value)
  case class LongVertexProperty(property: String, value: Long) extends VertexProperty(property, value)
  case class DoubleVertexProperty(property: String, value: Double) extends VertexProperty(property, value)
  case class TimestampVertexProperty(property: String, value: Timestamp) extends VertexProperty(property, value)
  case class BooleanVertexProperty(property: String, value: Boolean) extends VertexProperty(property, value)
  case class GeoVertexProperty(property: String, value: Geo) extends VertexProperty(property, value)
  case class PasswordVertexProperty(property: String, value: Password) extends VertexProperty(property, value)

  case class EdgeProperty(property: String)

  val edgeEncoder: Encoder[DGraphEdgeRow] = Encoders.product[DGraphEdgeRow]
  val nodeEncoder: Encoder[DGraphNodeRow] = Encoders.product[DGraphNodeRow]

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
      .as[DGraphNodeRow](nodeEncoder)
      .rdd
      .map(toVertex)

  def loadEdges(targets: String*)(implicit session: SparkSession): RDD[Edge[EdgeProperty]] =
    loadEdges(session.read, targets.map(Target): _*)

  def loadEdges(reader: DataFrameReader, targets: Target*): RDD[Edge[EdgeProperty]] =
    reader
      .format(EdgesSource)
      .load(targets.map(_.target): _*)
      .as[DGraphEdgeRow](edgeEncoder)
      .rdd
      .map(toEdge)

  def toVertex(row: DGraphNodeRow): (VertexId, VertexProperty) =
    (row.subject, toVertexProperty(row))

  def toEdge(row: DGraphEdgeRow): Edge[EdgeProperty] =
    Edge(row.subject, row.objectUid, EdgeProperty(row.predicate))
  
  def toVertexProperty(row: DGraphNodeRow): VertexProperty = row.objectType match {
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

    def dgraphEdges(targets: String*): RDD[Edge[EdgeProperty]] =
      graphx.loadEdges(reader, targets.map(Target): _*)

  }

}
