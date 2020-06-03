package uk.co.gresearch.spark.dgraph

import java.sql.Timestamp

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameReader, Row, SparkSession}
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

  def loadGraph(targets: Target*)(implicit session: SparkSession): Graph[VertexProperty, EdgeProperty] = {
    Graph(loadVertices(targets: _*), loadEdges(targets: _*))
  }

  def loadVertices(targets: Target*)(implicit session: SparkSession): RDD[(VertexId, VertexProperty)] = {
    import session.implicits._
    session
      .read
      .format(NodesSource)
      .load(targets.map(_.target): _*)
      .as[DGraphNodeRow]
      .rdd
      .map(toVertex)
  }

  def loadEdges(targets: Target*)(implicit session: SparkSession): RDD[Edge[EdgeProperty]] = {
    import session.implicits._
    session
      .read
      .format(EdgesSource)
      .load(targets.map(_.target): _*)
      .as[DGraphEdgeRow]
      .rdd
      .map(toEdge)
  }

  def toVertex(row: DGraphNodeRow): (VertexId, VertexProperty) =
    (row.subject, toVertexProperty(row))

  def toEdge(row: DGraphEdgeRow): Edge[EdgeProperty] =
    Edge(row.subject, row.objectUid, EdgeProperty(row.predicate))
  
  def toVertexProperty(row: DGraphNodeRow): VertexProperty = row.objectType match {
    case "string" => StringVertexProperty(row.predicate, row.objectString)
    case "long" => LongVertexProperty(row.predicate, row.objectLong)
    case "double" => DoubleVertexProperty(row.predicate, row.objectDouble)
    case "timestamp" => TimestampVertexProperty(row.predicate, row.objectTimestamp)
    case "boolean" => BooleanVertexProperty(row.predicate, row.objectBoolean)
    case "geo" => GeoVertexProperty(row.predicate, Geo(row.objectGeo))
    case "password" => PasswordVertexProperty(row.predicate, Password(row.objectPassword))
    case "default" => StringVertexProperty(row.predicate, row.objectString)
    case _ =>
      throw new IllegalArgumentException(s"Unsupported object type ${row.objectType} in node row: $row")
  }

  implicit class GraphSparkSession(session: SparkSession) {

    implicit val spark: SparkSession = session

    def loadGraph(targets: Seq[Target]): Graph[VertexProperty, EdgeProperty] =
      graphx.loadGraph(targets: _*)
  }

}
