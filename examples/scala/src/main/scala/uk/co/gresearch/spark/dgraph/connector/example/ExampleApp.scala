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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.GraphFrame

object ExampleApp {

  def main(args: Array[String]): Unit = {

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

    val target = "localhost:9080"

    {
      import uk.co.gresearch.spark.dgraph.graphx._
      val graph: Graph[VertexProperty, EdgeProperty] = spark.read.dgraph.graphx(target)
      val edges: RDD[Edge[EdgeProperty]] = spark.read.dgraph.edges(target)
      val vertices: RDD[(VertexId, VertexProperty)] = spark.read.dgraph.vertices(target)

      assert(graph.edges.count() == 12, graph.edges.count())
      assert(graph.vertices.count() == 11, graph.vertices.count())
      assert(edges.count() == 12, edges.count())
      assert(vertices.count() == 52, vertices.count())
    }

    {
      import uk.co.gresearch.spark.dgraph.graphframes._
      val graph: GraphFrame = spark.read.dgraph.graphframes(target)
      val edges: DataFrame = spark.read.dgraph.edges(target)
      val vertices: DataFrame = spark.read.dgraph.vertices(target)

      val triangles = graph.triangleCount.run().select($"id", $"count").orderBy($"id").as[(Long, Long)].collect().toSeq
      assert(triangles == Range(1, 12).map(i => (i, 0)), triangles)
      assert(edges.count() == 12)
      assert(vertices.count() == 11)
    }

    {
      import uk.co.gresearch.spark.dgraph.connector._
      val triples: DataFrame = spark.read.dgraph.triples(target)
      val edges: DataFrame = spark.read.dgraph.edges(target)
      val nodes: DataFrame = spark.read.dgraph.nodes(target)

      assert(triples.count() == 64, triples.count())
      assert(edges.count() == 12, edges.count())
      assert(nodes.count() == 52, nodes.count())
    }

  }

}
