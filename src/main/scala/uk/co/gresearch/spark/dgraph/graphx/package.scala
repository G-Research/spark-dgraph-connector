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

  implicit class GraphSparkSession(session: SparkSession) {

    implicit val spark: SparkSession = session

    def loadGraph(targets: Seq[Target]): Graph[VertexProperty, EdgeProperty] =
      graphx.loadGraph(targets: _*)
  }

}
