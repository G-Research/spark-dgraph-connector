/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.graphx

import java.sql.Timestamp

import org.apache.spark.graphx
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster

class TestGraphX extends FunSpec
  with SparkTestSession with DgraphTestCluster {

  describe("GraphX") {

    def doGraphTest(load: () => Graph[VertexProperty, EdgeProperty]): Unit = {
      val graph = load()
      val pageRank = graph.pageRank(0.0001)
      val vertices = pageRank.vertices.collect()
      assert(vertices === Seq(
        (10,0.7968127490039841),
        (1,0.7968127490039841),
        (2,1.3047808764940239),
        (3,0.9661354581673308),
        (4,0.9661354581673308),
        (5,0.7968127490039841),
        (6,0.7968127490039841),
        (7,1.3047808764940239),
        (8,1.3047808764940239),
        (9,0.9661354581673308),
      ))
    }

    def doVertexTest(load: () => RDD[(graphx.VertexId, VertexProperty)]): Unit = {
      val vertices = load()
      assert(vertices.collect().sortBy(v => (v._1, v._2.property, v._2.value.toString)) === Seq(
        (1, StringVertexProperty("dgraph.type", "Film")),
        (1, StringVertexProperty("name", "Star Trek: The Motion Picture")),
        (1, TimestampVertexProperty("release_date", Timestamp.valueOf("1979-12-07 00:00:00.0"))),
        (1, DoubleVertexProperty("revenue", 1.39E8)),
        (1, LongVertexProperty("running_time", 132)),
        (2, StringVertexProperty("dgraph.type", "Person")),
        (2, StringVertexProperty("name", "Princess Leia")),
        (3, StringVertexProperty("dgraph.type", "Person")),
        (3, StringVertexProperty("name", "George Lucas")),
        (4, StringVertexProperty("dgraph.type", "Person")),
        (4, StringVertexProperty("name", "Irvin Kernshner")),
        (5, StringVertexProperty("dgraph.type", "Film")),
        (5, StringVertexProperty("name", "Star Wars: Episode IV - A New Hope")),
        (5, TimestampVertexProperty("release_date", Timestamp.valueOf("1977-05-25 00:00:00.0"))),
        (5, DoubleVertexProperty("revenue", 7.75E8)),
        (5, LongVertexProperty("running_time", 121)),
        (6, StringVertexProperty("dgraph.type", "Film")),
        (6, StringVertexProperty("name", "Star Wars: Episode V - The Empire Strikes Back")),
        (6, TimestampVertexProperty("release_date", Timestamp.valueOf("1980-05-21 00:00:00.0"))),
        (6, DoubleVertexProperty("revenue", 5.34E8)),
        (6, LongVertexProperty("running_time", 124)),
        (7, StringVertexProperty("dgraph.type", "Person")),
        (7, StringVertexProperty("name", "Luke Skywalker")),
        (8, StringVertexProperty("dgraph.type", "Person")),
        (8, StringVertexProperty("name", "Han Solo")),
        (9, StringVertexProperty("dgraph.type", "Person")),
        (9, StringVertexProperty("name", "Richard Marquand")),
        (10, StringVertexProperty("dgraph.type", "Film")),
        (10, StringVertexProperty("name", "Star Wars: Episode VI - Return of the Jedi")),
        (10, TimestampVertexProperty("release_date", Timestamp.valueOf("1983-05-25 00:00:00.0"))),
        (10, DoubleVertexProperty("revenue", 5.72E8)),
        (10, LongVertexProperty("running_time", 131)),
      ))
    }

    def doEdgeTest(load: () => RDD[Edge[EdgeProperty]]): Unit = {
      val edges = load()
      assert(edges.collect().sortBy(e => (e.srcId, e.dstId, e.attr.property)) === Seq(
        Edge(5, 2, EdgeProperty("starring")),
        Edge(5, 3, EdgeProperty("director")),
        Edge(5, 7, EdgeProperty("starring")),
        Edge(5, 8, EdgeProperty("starring")),
        Edge(6, 2, EdgeProperty("starring")),
        Edge(6, 4, EdgeProperty("director")),
        Edge(6, 7, EdgeProperty("starring")),
        Edge(6, 8, EdgeProperty("starring")),
        Edge(10, 2, EdgeProperty("starring")),
        Edge(10, 7, EdgeProperty("starring")),
        Edge(10, 8, EdgeProperty("starring")),
        Edge(10, 9, EdgeProperty("director")),
      ))
    }

    Seq(
      ("target", Seq("localhost:9080")),
      ("targets", Seq("localhost:9080", "127.0.0.1:9080"))
    ).foreach{case (test, targets) =>

      it(s"should load dgraph from $test via implicit session") {
        doGraphTest(() => loadGraph(targets: _*))
      }

      it(s"should load dgraph from $test via reader") {
        doGraphTest(() => spark.read.dgraph(targets: _*))
      }

      it(s"should load vertices from $test via implicit session") {
        doVertexTest(() => loadVertices(targets: _*))
      }

      it(s"should load vertices from $test via reader") {
        doVertexTest(() => spark.read.dgraphVertices(targets: _*))
      }

      it(s"should load edges from $test via implicit session") {
        doEdgeTest(() => loadEdges(targets: _*))
      }

      it(s"should load edges from $test via reader") {
        doEdgeTest(() => spark.read.dgraphEdges(targets: _*))
      }

    }

  }

}
