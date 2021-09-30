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

import org.apache.spark.graphx.{Edge, EdgeRDD, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector.{ConnectorSparkTestSession, Target}
import uk.co.gresearch.spark.dgraph.graphx.TestGraphX.dgraphVertex

import java.sql.Timestamp

class TestGraphX extends AnyFunSpec
  with ConnectorSparkTestSession with DgraphTestCluster {

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

  describe("GraphX") {

    def doGraphTest(load: () => Graph[VertexProperty, EdgeProperty]): Unit = {
      val graph = removeDgraphNodes(load())
      val pageRank = graph.pageRank(0.0001)
      val vertices = pageRank.vertices.collect().map(t => (t._1, t._2.toFloat)).sortBy(_._1)
      val expected = Seq(
        (dgraph.st1,0.7968128f),
        (dgraph.leia,1.3047808f),
        (dgraph.lucas,0.96613544f),
        (dgraph.irvin,0.96613544f),
        (dgraph.sw1,0.7968128f),
        (dgraph.sw2,0.7968128f),
        (dgraph.luke,1.3047808f),
        (dgraph.han,1.3047808f),
        (dgraph.richard,0.96613544f),
        (dgraph.sw3,0.7968128f),
      ).sortBy(_._1)
      assert(vertices === expected)
    }

    def doVertexTest(load: () => RDD[(VertexId, VertexProperty)]): Unit = {
      val vertices = removeDgraphVertices(load()).collect().sortBy(v => (v._1, v._2.property))
      val expected = Seq(
        (dgraph.st1, StringVertexProperty("dgraph.type", "Film")),
        (dgraph.st1, StringVertexProperty("title@en", "Star Trek: The Motion Picture")),
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
        (dgraph.sw1, StringVertexProperty("title", "Star Wars: Episode IV - A New Hope")),
        (dgraph.sw1, StringVertexProperty("title@en", "Star Wars: Episode IV - A New Hope")),
        (dgraph.sw1, StringVertexProperty("title@hu", "Csillagok háborúja IV: Egy új remény")),
        (dgraph.sw1, StringVertexProperty("title@be", "Зорныя войны. Эпізод IV: Новая надзея")),
        (dgraph.sw1, StringVertexProperty("title@cs", "Star Wars: Epizoda IV – Nová naděje")),
        (dgraph.sw1, StringVertexProperty("title@br", "Star Wars Lodenn 4: Ur Spi Nevez")),
        (dgraph.sw1, StringVertexProperty("title@de", "Krieg der Sterne")),
        (dgraph.sw1, TimestampVertexProperty("release_date", Timestamp.valueOf("1977-05-25 00:00:00.0"))),
        (dgraph.sw1, DoubleVertexProperty("revenue", 7.75E8)),
        (dgraph.sw1, LongVertexProperty("running_time", 121)),
        (dgraph.sw2, StringVertexProperty("dgraph.type", "Film")),
        (dgraph.sw2, StringVertexProperty("title", "Star Wars: Episode V - The Empire Strikes Back")),
        (dgraph.sw2, StringVertexProperty("title@en", "Star Wars: Episode V - The Empire Strikes Back")),
        (dgraph.sw2, StringVertexProperty("title@ka", "ვარსკვლავური ომები, ეპიზოდი V: იმპერიის საპასუხო დარტყმა")),
        (dgraph.sw2, StringVertexProperty("title@ko", "제국의 역습")),
        (dgraph.sw2, StringVertexProperty("title@iw", "מלחמת הכוכבים - פרק 5: האימפריה מכה שנית")),
        (dgraph.sw2, StringVertexProperty("title@de", "Das Imperium schlägt zurück")),
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
        (dgraph.sw3, StringVertexProperty("title", "Star Wars: Episode VI - Return of the Jedi")),
        (dgraph.sw3, StringVertexProperty("title@en", "Star Wars: Episode VI - Return of the Jedi")),
        (dgraph.sw3, StringVertexProperty("title@zh", "星際大戰六部曲：絕地大反攻")),
        (dgraph.sw3, StringVertexProperty("title@th", "สตาร์ วอร์ส เอพพิโซด 6: การกลับมาของเจได")),
        (dgraph.sw3, StringVertexProperty("title@fa", "بازگشت جدای")),
        (dgraph.sw3, StringVertexProperty("title@ar", "حرب النجوم الجزء السادس: عودة الجيداي")),
        (dgraph.sw3, StringVertexProperty("title@de", "Die Rückkehr der Jedi-Ritter")),
        (dgraph.sw3, TimestampVertexProperty("release_date", Timestamp.valueOf("1983-05-25 00:00:00.0"))),
        (dgraph.sw3, DoubleVertexProperty("revenue", 5.72E8)),
        (dgraph.sw3, LongVertexProperty("running_time", 131)),
      ).sortBy(v => (v._1, v._2.property))
      assert(vertices === expected)
    }

    def doEdgeTest(load: () => RDD[Edge[EdgeProperty]]): Unit = {
      val dgraphVertexIds = reader.dgraph.vertices(dgraph.target).filter(v => dgraphVertex(v._2)).map(_._1).collect().toSet
      val edges = removeDgraphEdges(load(), dgraphVertexIds).collect().sortBy(e => (e.srcId, e.dstId))
      val expected = Seq(
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
      ).sortBy(e => (e.srcId, e.dstId))
      assert(edges === expected)
    }

    Seq(
      ("target", () => Seq(dgraph.target)),
      ("targets", () => Seq(dgraph.target, dgraph.targetLocalIp))
    ).foreach{case (test, targets) =>

      it(s"should load dgraph from $test via implicit session") {
        doGraphTest(() => loadGraph(reader, targets().map(Target): _*))
      }

      it(s"should load dgraph from $test via reader") {
        doGraphTest(() => reader.dgraph.graphx(targets(): _*))
      }

      it(s"should load vertices from $test via implicit session") {
        doVertexTest(() => loadVertices(reader, targets().map(Target): _*))
      }

      it(s"should load vertices from $test via reader") {
        doVertexTest(() => reader.dgraph.vertices(targets(): _*))
      }

      it(s"should load edges from $test via implicit session") {
        doEdgeTest(() => loadEdges(reader, targets().map(Target): _*))
      }

      it(s"should load edges from $test via reader") {
        doEdgeTest(() => reader.dgraph.edges(targets(): _*))
      }

    }

  }

}

object TestGraphX {

  def dgraphVertex(property: VertexProperty): Boolean =
    property.property == "dgraph.type" && Option(property.value).exists(_.toString.startsWith("dgraph."))

}
