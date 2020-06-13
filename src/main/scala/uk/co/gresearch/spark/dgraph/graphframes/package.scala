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
        .dgraphNodes(targets.head, targets.tail: _*)
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
      .dgraphEdges(targets.head, targets.tail: _*)
      .select(
        col("subject").as("src"),
        col("objectUid").as("dst"),
        col("predicate")
      )

  implicit class GraphFrameDataFrameReader(reader: DataFrameReader) {

    def dgraph(targets: String*): GraphFrame =
      graphframes.loadGraph(reader, targets: _*)

    def dgraphVertices(targets: String*): DataFrame =
      graphframes.loadVertices(reader, targets: _*)

    def dgraphEdges(targets: String*): DataFrame =
      graphframes.loadEdges(reader, targets: _*)

  }

}
