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

package uk.co.gresearch.spark.dgraph.connector.example

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.scalactic.TripleEquals
import org.graphframes.GraphFrame
import uk.co.gresearch.spark.dgraph.connector.{IncludeReservedPredicatesOption, TypedNode}
import uk.co.gresearch.spark.dgraph.graphx.{EdgeProperty, VertexProperty}

object ExampleApp {

  def dgraphVertex(property: VertexProperty): Boolean =
    property.property == "dgraph.type" && Option(property.value).exists(_.toString.startsWith("dgraph."))

  def main(args: Array[String]): Unit = {
    import TripleEquals._

    val spark: SparkSession = {
      SparkSession
        .builder()
        .master(s"local[*]")
        .appName("spark dgraph example")
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.local.dir", ".")
        .getOrCreate()
    }
    import spark.implicits._

    def reader: DataFrameReader = spark.read.option(IncludeReservedPredicatesOption, "dgraph.type")

    val target = "localhost:9080"

    {
      def removeDgraphEdges[E](edges: RDD[Edge[E]], dgraphVertexIds: Set[Long]): RDD[Edge[E]] =
        edges.filter(e => !dgraphVertexIds.contains(e.srcId) && !dgraphVertexIds.contains(e.dstId))

      def removeDgraphVertices(vertices: RDD[(VertexId, VertexProperty)]): RDD[(VertexId, VertexProperty)] =
        vertices.filter(v => !dgraphVertex(v._2))

      def removeDgraphNodes(graph: Graph[VertexProperty, EdgeProperty]): Graph[VertexProperty, EdgeProperty] = {
        val droppedVertexIds = graph.vertices.filter(v => dgraphVertex(v._2)).collect().map(_._1).toSet
        val filteredVertexes = graph.vertices.filter(v => !dgraphVertex(v._2))
        val filteredEdges = removeDgraphEdges(graph.edges, droppedVertexIds)
        Graph(filteredVertexes, filteredEdges)
      }

      import uk.co.gresearch.spark.dgraph.graphx._
      val graph: Graph[VertexProperty, EdgeProperty] = removeDgraphNodes(reader.dgraph.graphx(target))
      val vertices: RDD[(VertexId, VertexProperty)] = removeDgraphVertices(reader.dgraph.vertices(target))
      val dgraphVertexIds = reader.dgraph.vertices(target).filter(v => dgraphVertex(v._2)).map(_._1).collect().toSet
      val edges: RDD[Edge[EdgeProperty]] = removeDgraphEdges(reader.dgraph.edges(target), dgraphVertexIds)

      assert(graph.edges.count() === 12)
      assert(graph.vertices.count() === 10)
      assert(edges.count() === 12)
      assert(vertices.count() === 49)
    }

    {
      val isDgraphNode: Column = $"`dgraph.type`".startsWith("dgraph.")
      def isDgraphEdge(dgraphVertexIds: Set[Long]): Column = $"`src`".isin(dgraphVertexIds.toSeq: _*) || $"dst".isin(dgraphVertexIds.toSeq: _*)

      def removeDgraphEdges(edges: DataFrame, dgraphVertexIds: Set[Long]): DataFrame =
        edges.where(!isDgraphEdge(dgraphVertexIds))

      def removeDgraphVertices(vertices: DataFrame): DataFrame =
        vertices.where(!isDgraphNode)

      def removeDgraphNodes(graph: GraphFrame): GraphFrame = {
        val droppedVertexIds = graph.vertices.where(isDgraphNode).select($"id").as[Long].collect().toSet
        val filteredVertexes = graph.vertices.where(!isDgraphNode)
        val filteredEdges = removeDgraphEdges(graph.edges, droppedVertexIds)
        GraphFrame(filteredVertexes, filteredEdges)
      }

      import uk.co.gresearch.spark.dgraph.graphframes._
      val graph: GraphFrame = removeDgraphNodes(reader.dgraph.graphframes(target))
      val vertices: DataFrame = removeDgraphVertices(reader.dgraph.vertices(target))
      val dgraphNodes = reader.dgraph.vertices(target).where(isDgraphNode).select($"id").as[Long].collect().toSet
      val edges: DataFrame = removeDgraphEdges(reader.dgraph.edges(target), dgraphNodes)

      val triangles = graph.triangleCount.run().select($"id", $"count").orderBy($"id").as[(Long, Long)].collect().toSeq
      assert(triangles.length === 10)
      assert(edges.count() === 12)
      assert(vertices.count() === 10)
    }

    {
      def removeDgraphTriples[T](triples: Dataset[T]): Dataset[T] = {
        import triples.sparkSession.implicits._
        val dgraphNodeUids = triples.where($"predicate" === "dgraph.type" && $"objectString".startsWith("dgraph.")).select($"subject").distinct().as[Long].collect()
        triples.where(!$"subject".isin(dgraphNodeUids: _*))
      }

      def removeDgraphTypedNodes(nodes: Dataset[TypedNode]): Dataset[TypedNode] = {
        val dgraphNodeUids =
          nodes
            .where($"predicate" === "dgraph.type" && $"objectString".startsWith("dgraph."))
            .select($"subject").distinct().as[Long].collect()
        nodes.where(!$"subject".isin(dgraphNodeUids: _*))
      }

      import uk.co.gresearch.spark.dgraph.connector._
      val triples: DataFrame = removeDgraphTriples(reader.dgraph.triples(target))
      val edges: DataFrame = reader.dgraph.edges(target)
      val nodes: DataFrame = removeDgraphTypedNodes(reader.dgraph.nodes(target).as[TypedNode]).toDF()

      assert(triples.count() === 61)
      assert(edges.count() === 12)
      assert(nodes.count() === 49)
    }

  }

}
