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

import java.sql.Timestamp

import org.apache.spark.graphx
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster

class TestGraphX extends AnyFunSpec
  with SparkTestSession with DgraphTestCluster {

  describe("GraphX") {

    def doGraphTest(load: () => Graph[VertexProperty, EdgeProperty]): Unit = {
      val graph = load()
      val pageRank = graph.pageRank(0.0001)
      val vertices = pageRank.vertices.collect().map(t => (t._1, t._2.toFloat)).toSet
      val expected = Set(
        (dgraph.graphQlSchema,0.8118081180811808),
        (dgraph.st1,0.8118081180811808),
        (dgraph.leia,1.3293357933579335),
        (dgraph.lucas,0.9843173431734319),
        (dgraph.irvin,0.9843173431734319),
        (dgraph.sw1,0.8118081180811808),
        (dgraph.sw2,0.8118081180811808),
        (dgraph.luke,1.3293357933579335),
        (dgraph.han,1.3293357933579335),
        (dgraph.richard,0.9843173431734319),
        (dgraph.sw3,0.8118081180811808),
      ).map(t => (t._1, t._2.toFloat))
      assert(vertices === expected)
    }

    def doVertexTest(load: () => RDD[(graphx.VertexId, VertexProperty)]): Unit = {
      val vertices = load().collect().toSet
      val expected = Set(
        (dgraph.graphQlSchema,StringVertexProperty("dgraph.type", "dgraph.graphql")),
        (dgraph.graphQlSchema,StringVertexProperty("dgraph.graphql.xid", "dgraph.graphql.schema")),
        (dgraph.graphQlSchema,StringVertexProperty("dgraph.graphql.schema", "")),
        (dgraph.st1, StringVertexProperty("dgraph.type", "Film")),
        (dgraph.st1, StringVertexProperty("name", "Star Trek: The Motion Picture")),
        (dgraph.st1, TimestampVertexProperty("release_date", Timestamp.valueOf("1979-12-07 00:00:00.0"))),
        (dgraph.st1, DoubleVertexProperty("revenue", 1.39E8)),
        (dgraph.st1, LongVertexProperty("running_time", 132)),
        (dgraph.leia, StringVertexProperty("dgraph.type", "Person")),
        (dgraph.leia, StringVertexProperty("name", "Princess Leia")),
        (dgraph.lucas, StringVertexProperty("dgraph.type", "Person")),
        (dgraph.lucas, StringVertexProperty("name", "George Lucas")),
        (dgraph.irvin, StringVertexProperty("dgraph.type", "Person")),
        (dgraph.irvin, StringVertexProperty("name", "Irvin Kernshner")),
        (dgraph.sw1, StringVertexProperty("dgraph.type", "Film")),
        (dgraph.sw1, StringVertexProperty("name", "Star Wars: Episode IV - A New Hope")),
        (dgraph.sw1, TimestampVertexProperty("release_date", Timestamp.valueOf("1977-05-25 00:00:00.0"))),
        (dgraph.sw1, DoubleVertexProperty("revenue", 7.75E8)),
        (dgraph.sw1, LongVertexProperty("running_time", 121)),
        (dgraph.sw2, StringVertexProperty("dgraph.type", "Film")),
        (dgraph.sw2, StringVertexProperty("name", "Star Wars: Episode V - The Empire Strikes Back")),
        (dgraph.sw2, TimestampVertexProperty("release_date", Timestamp.valueOf("1980-05-21 00:00:00.0"))),
        (dgraph.sw2, DoubleVertexProperty("revenue", 5.34E8)),
        (dgraph.sw2, LongVertexProperty("running_time", 124)),
        (dgraph.luke, StringVertexProperty("dgraph.type", "Person")),
        (dgraph.luke, StringVertexProperty("name", "Luke Skywalker")),
        (dgraph.han, StringVertexProperty("dgraph.type", "Person")),
        (dgraph.han, StringVertexProperty("name", "Han Solo")),
        (dgraph.richard, StringVertexProperty("dgraph.type", "Person")),
        (dgraph.richard, StringVertexProperty("name", "Richard Marquand")),
        (dgraph.sw3, StringVertexProperty("dgraph.type", "Film")),
        (dgraph.sw3, StringVertexProperty("name", "Star Wars: Episode VI - Return of the Jedi")),
        (dgraph.sw3, TimestampVertexProperty("release_date", Timestamp.valueOf("1983-05-25 00:00:00.0"))),
        (dgraph.sw3, DoubleVertexProperty("revenue", 5.72E8)),
        (dgraph.sw3, LongVertexProperty("running_time", 131)),
      )
      assert(vertices === expected)
    }

    def doEdgeTest(load: () => RDD[Edge[EdgeProperty]]): Unit = {
      val edges = load().collect().toSet
      val expected = Set(
        Edge(dgraph.sw1, dgraph.leia, EdgeProperty("starring")),
        Edge(dgraph.sw1, dgraph.lucas, EdgeProperty("director")),
        Edge(dgraph.sw1, dgraph.luke, EdgeProperty("starring")),
        Edge(dgraph.sw1, dgraph.han, EdgeProperty("starring")),
        Edge(dgraph.sw2, dgraph.leia, EdgeProperty("starring")),
        Edge(dgraph.sw2, dgraph.irvin, EdgeProperty("director")),
        Edge(dgraph.sw2, dgraph.luke, EdgeProperty("starring")),
        Edge(dgraph.sw2, dgraph.han, EdgeProperty("starring")),
        Edge(dgraph.sw3, dgraph.leia, EdgeProperty("starring")),
        Edge(dgraph.sw3, dgraph.luke, EdgeProperty("starring")),
        Edge(dgraph.sw3, dgraph.han, EdgeProperty("starring")),
        Edge(dgraph.sw3, dgraph.richard, EdgeProperty("director")),
      )
      assert(edges === expected)
    }

    Seq(
      ("target", () => Seq(dgraph.target)),
      ("targets", () => Seq(dgraph.target, dgraph.targetLocalIp))
    ).foreach{case (test, targets) =>

      it(s"should load dgraph from $test via implicit session") {
        doGraphTest(() => loadGraph(targets(): _*))
      }

      it(s"should load dgraph from $test via reader") {
        doGraphTest(() => spark.read.dgraph.graphx(targets(): _*))
      }

      it(s"should load vertices from $test via implicit session") {
        doVertexTest(() => loadVertices(targets(): _*))
      }

      it(s"should load vertices from $test via reader") {
        doVertexTest(() => spark.read.dgraph.vertices(targets(): _*))
      }

      it(s"should load edges from $test via implicit session") {
        doEdgeTest(() => loadEdges(targets(): _*))
      }

      it(s"should load edges from $test via reader") {
        doEdgeTest(() => spark.read.dgraph.edges(targets(): _*))
      }

    }

  }

}
