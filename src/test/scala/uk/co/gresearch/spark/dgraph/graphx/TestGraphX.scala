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
      val vertices = pageRank.vertices.collect().map(t => (t._1, t._2.toFloat)).toSet
      val expected = Set(
        (graphQlSchema,0.8118081180811808),
        (st1,0.8118081180811808),
        (leia,1.3293357933579335),
        (lucas,0.9843173431734319),
        (irvin,0.9843173431734319),
        (sw1,0.8118081180811808),
        (sw2,0.8118081180811808),
        (luke,1.3293357933579335),
        (han,1.3293357933579335),
        (richard,0.9843173431734319),
        (sw3,0.8118081180811808),
      ).map(t => (t._1, t._2.toFloat))
      assert(vertices === expected)
    }

    def doVertexTest(load: () => RDD[(graphx.VertexId, VertexProperty)]): Unit = {
      val vertices = load().collect().toSet
      val expected = Set(
        (graphQlSchema,StringVertexProperty("dgraph.type", "dgraph.graphql")),
        (graphQlSchema,StringVertexProperty("dgraph.graphql.xid", "dgraph.graphql.schema")),
        (graphQlSchema,StringVertexProperty("dgraph.graphql.schema", "")),
        (st1, StringVertexProperty("dgraph.type", "Film")),
        (st1, StringVertexProperty("name", "Star Trek: The Motion Picture")),
        (st1, TimestampVertexProperty("release_date", Timestamp.valueOf("1979-12-07 00:00:00.0"))),
        (st1, DoubleVertexProperty("revenue", 1.39E8)),
        (st1, LongVertexProperty("running_time", 132)),
        (leia, StringVertexProperty("dgraph.type", "Person")),
        (leia, StringVertexProperty("name", "Princess Leia")),
        (lucas, StringVertexProperty("dgraph.type", "Person")),
        (lucas, StringVertexProperty("name", "George Lucas")),
        (irvin, StringVertexProperty("dgraph.type", "Person")),
        (irvin, StringVertexProperty("name", "Irvin Kernshner")),
        (sw1, StringVertexProperty("dgraph.type", "Film")),
        (sw1, StringVertexProperty("name", "Star Wars: Episode IV - A New Hope")),
        (sw1, TimestampVertexProperty("release_date", Timestamp.valueOf("1977-05-25 00:00:00.0"))),
        (sw1, DoubleVertexProperty("revenue", 7.75E8)),
        (sw1, LongVertexProperty("running_time", 121)),
        (sw2, StringVertexProperty("dgraph.type", "Film")),
        (sw2, StringVertexProperty("name", "Star Wars: Episode V - The Empire Strikes Back")),
        (sw2, TimestampVertexProperty("release_date", Timestamp.valueOf("1980-05-21 00:00:00.0"))),
        (sw2, DoubleVertexProperty("revenue", 5.34E8)),
        (sw2, LongVertexProperty("running_time", 124)),
        (luke, StringVertexProperty("dgraph.type", "Person")),
        (luke, StringVertexProperty("name", "Luke Skywalker")),
        (han, StringVertexProperty("dgraph.type", "Person")),
        (han, StringVertexProperty("name", "Han Solo")),
        (richard, StringVertexProperty("dgraph.type", "Person")),
        (richard, StringVertexProperty("name", "Richard Marquand")),
        (sw3, StringVertexProperty("dgraph.type", "Film")),
        (sw3, StringVertexProperty("name", "Star Wars: Episode VI - Return of the Jedi")),
        (sw3, TimestampVertexProperty("release_date", Timestamp.valueOf("1983-05-25 00:00:00.0"))),
        (sw3, DoubleVertexProperty("revenue", 5.72E8)),
        (sw3, LongVertexProperty("running_time", 131)),
      )
      assert(vertices === expected)
    }

    def doEdgeTest(load: () => RDD[Edge[EdgeProperty]]): Unit = {
      val edges = load().collect().toSet
      val expected = Set(
        Edge(sw1, leia, EdgeProperty("starring")),
        Edge(sw1, lucas, EdgeProperty("director")),
        Edge(sw1, luke, EdgeProperty("starring")),
        Edge(sw1, han, EdgeProperty("starring")),
        Edge(sw2, leia, EdgeProperty("starring")),
        Edge(sw2, irvin, EdgeProperty("director")),
        Edge(sw2, luke, EdgeProperty("starring")),
        Edge(sw2, han, EdgeProperty("starring")),
        Edge(sw3, leia, EdgeProperty("starring")),
        Edge(sw3, luke, EdgeProperty("starring")),
        Edge(sw3, han, EdgeProperty("starring")),
        Edge(sw3, richard, EdgeProperty("director")),
      )
      assert(edges === expected)
    }

    Seq(
      ("target", () => Seq(cluster.grpc)),
      ("targets", () => Seq(cluster.grpc, cluster.grpcLocalIp))
    ).foreach{case (test, targets) =>

      it(s"should load dgraph from $test via implicit session") {
        doGraphTest(() => loadGraph(targets(): _*))
      }

      it(s"should load dgraph from $test via reader") {
        doGraphTest(() => spark.read.dgraph(targets(): _*))
      }

      it(s"should load vertices from $test via implicit session") {
        doVertexTest(() => loadVertices(targets(): _*))
      }

      it(s"should load vertices from $test via reader") {
        doVertexTest(() => spark.read.dgraphVertices(targets(): _*))
      }

      it(s"should load edges from $test via implicit session") {
        doEdgeTest(() => loadEdges(targets(): _*))
      }

      it(s"should load edges from $test via reader") {
        doEdgeTest(() => spark.read.dgraphEdges(targets(): _*))
      }

    }

  }

}
