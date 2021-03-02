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

package uk.co.gresearch.spark.dgraph.graphframes

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, FloatType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.graphframes.GraphFrame
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.DgraphTestCluster
import uk.co.gresearch.spark.dgraph.connector.ConnectorSparkTestSession

import java.sql.Timestamp
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

class TestGraphFrames extends AnyFunSpec
  with ConnectorSparkTestSession with DgraphTestCluster {

  import spark.implicits._

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

  val vertexSchema: StructType = StructType(Seq(
    StructField("id", LongType, nullable = false),
    StructField("dgraph_type", StringType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("release__date", TimestampType, nullable = true),
    StructField("revenue", DoubleType, nullable = true),
    StructField("running__time", LongType, nullable = true),
    StructField("title", StringType, nullable = true)
  ))
  val pageRankVertexSchema: StructType = StructType(
    vertexSchema.fields :+ StructField("pagerank", FloatType, nullable = true)
  )

  val edgeSchema: StructType = StructType(Seq(
    StructField("src", LongType, nullable = false),
    StructField("dst", LongType, nullable = false),
    StructField("predicate", StringType, nullable = false)
  ))
  val pageRankEdgeSchema: StructType = StructType(
    edgeSchema.fields :+ StructField("weight", DoubleType, nullable = true)
  )

  val pageRankSchema: StructType = StructType(Seq(
    StructField("src", pageRankVertexSchema, nullable = false),
    StructField("edge", pageRankEdgeSchema, nullable = false),
    StructField("dst", pageRankVertexSchema, nullable = false)
  ))

  describe("GraphFrames") {

    def doGraphTest(load: () => GraphFrame): Unit = {
      val graph = removeDgraphNodes(load())
      val pageRank = graph.pageRank.maxIter(10)
      val doubleTriples = pageRank.run().triplets

      // pagerank has double precision but is not numerically stable
      // we cast those doubles to floats
      val schema = doubleTriples.schema
      val srcFields = schema.fields(0).dataType.asInstanceOf[StructType].fields
      val edge = schema.fields(1)
      val dstFields = schema.fields(2).dataType.asInstanceOf[StructType]
      val srcFloat = StructType(srcFields.dropRight(1) ++ srcFields.takeRight(1).map(f => f.copy(dataType = FloatType)))
      val dstFloat = StructType(dstFields.dropRight(1) ++ dstFields.takeRight(1).map(f => f.copy(dataType = FloatType)))

      // cast pagerank columns to float
      val triplets = doubleTriples.select(
        col("src").cast(srcFloat),
        col("edge").cast(edge.dataType),
        col("dst").cast(dstFloat)
      ).collect().sortBy(row => (row.getAs[Row](0).getLong(0), row.getAs[Row](2).getLong(0)))

      val expected = Seq(
        new GenericRowWithSchema(Array(
          new GenericRowWithSchema(Array(dgraph.sw1, "Film", null, Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, "Star Wars: Episode IV - A New Hope", 0.7968128f), pageRankVertexSchema),
          new GenericRowWithSchema(Array(dgraph.sw1, dgraph.leia, "starring", 0.25), pageRankEdgeSchema),
          new GenericRowWithSchema(Array(dgraph.leia, "Person", "Princess Leia", null, null, null, null, 1.3047808f), pageRankVertexSchema),
        ), pageRankSchema),
        new GenericRowWithSchema(Array(
          new GenericRowWithSchema(Array(dgraph.sw2, "Film", null, Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, "Star Wars: Episode V - The Empire Strikes Back", 0.7968128f), pageRankVertexSchema),
          new GenericRowWithSchema(Array(dgraph.sw2, dgraph.leia, "starring", 0.25), pageRankEdgeSchema),
          new GenericRowWithSchema(Array(dgraph.leia, "Person", "Princess Leia", null, null, null, null, 1.3047808f), pageRankVertexSchema),
        ), pageRankSchema),
        new GenericRowWithSchema(Array(
          new GenericRowWithSchema(Array(dgraph.sw3, "Film", null, Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, "Star Wars: Episode VI - Return of the Jedi", 0.7968128f), pageRankVertexSchema),
          new GenericRowWithSchema(Array(dgraph.sw3, dgraph.leia, "starring", 0.25), pageRankEdgeSchema),
          new GenericRowWithSchema(Array(dgraph.leia, "Person", "Princess Leia", null, null, null, null, 1.3047808f), pageRankVertexSchema),
        ), pageRankSchema),
        new GenericRowWithSchema(Array(
          new GenericRowWithSchema(Array(dgraph.sw2, "Film", null, Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, "Star Wars: Episode V - The Empire Strikes Back", 0.7968128f), pageRankVertexSchema),
          new GenericRowWithSchema(Array(dgraph.sw2, dgraph.irvin, "director", 0.25), pageRankEdgeSchema),
          new GenericRowWithSchema(Array(dgraph.irvin, "Person", "Irvin Kernshner", null, null, null, null, 0.96613544f), pageRankVertexSchema),
        ), pageRankSchema),
        new GenericRowWithSchema(Array(
          new GenericRowWithSchema(Array(dgraph.sw1, "Film", null, Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, "Star Wars: Episode IV - A New Hope", 0.7968128f), pageRankVertexSchema),
          new GenericRowWithSchema(Array(dgraph.sw1, dgraph.han, "starring", 0.25), pageRankEdgeSchema),
          new GenericRowWithSchema(Array(dgraph.han, "Person", "Han Solo", null, null, null, null, 1.3047808f), pageRankVertexSchema),
        ), pageRankSchema),
        new GenericRowWithSchema(Array(
          new GenericRowWithSchema(Array(dgraph.sw2, "Film", null, Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, "Star Wars: Episode V - The Empire Strikes Back", 0.7968128f), pageRankVertexSchema),
          new GenericRowWithSchema(Array(dgraph.sw2, dgraph.han, "starring", 0.25), pageRankEdgeSchema),
          new GenericRowWithSchema(Array(dgraph.han, "Person", "Han Solo", null, null, null, null, 1.3047808f), pageRankVertexSchema),
        ), pageRankSchema),
        new GenericRowWithSchema(Array(
          new GenericRowWithSchema(Array(dgraph.sw3, "Film", null, Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, "Star Wars: Episode VI - Return of the Jedi", 0.7968128f), pageRankVertexSchema),
          new GenericRowWithSchema(Array(dgraph.sw3, dgraph.han, "starring", 0.25), pageRankEdgeSchema),
          new GenericRowWithSchema(Array(dgraph.han, "Person", "Han Solo", null, null, null, null, 1.3047808f), pageRankVertexSchema),
        ), pageRankSchema),
        new GenericRowWithSchema(Array(
          new GenericRowWithSchema(Array(dgraph.sw1, "Film", null, Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, "Star Wars: Episode IV - A New Hope", 0.7968128f), pageRankVertexSchema),
          new GenericRowWithSchema(Array(dgraph.sw1, dgraph.lucas, "director", 0.25), pageRankEdgeSchema),
          new GenericRowWithSchema(Array(dgraph.lucas, "Person", "George Lucas", null, null, null, null, 0.96613544f), pageRankVertexSchema),
        ), pageRankSchema),
        new GenericRowWithSchema(Array(
          new GenericRowWithSchema(Array(dgraph.sw1, "Film", null, Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, "Star Wars: Episode IV - A New Hope", 0.7968128f), pageRankVertexSchema),
          new GenericRowWithSchema(Array(dgraph.sw1, dgraph.luke, "starring", 0.25), pageRankEdgeSchema),
          new GenericRowWithSchema(Array(dgraph.luke, "Person", "Luke Skywalker", null, null, null, null, 1.3047808f), pageRankVertexSchema),
        ), pageRankSchema),
        new GenericRowWithSchema(Array(
          new GenericRowWithSchema(Array(dgraph.sw2, "Film", null, Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, "Star Wars: Episode V - The Empire Strikes Back", 0.7968128f), pageRankVertexSchema),
          new GenericRowWithSchema(Array(dgraph.sw2, dgraph.luke, "starring", 0.25), pageRankEdgeSchema),
          new GenericRowWithSchema(Array(dgraph.luke, "Person", "Luke Skywalker", null, null, null, null, 1.3047808f), pageRankVertexSchema),
        ), pageRankSchema),
        new GenericRowWithSchema(Array(
          new GenericRowWithSchema(Array(dgraph.sw3, "Film", null, Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, "Star Wars: Episode VI - Return of the Jedi", 0.7968128f), pageRankVertexSchema),
          new GenericRowWithSchema(Array(dgraph.sw3, dgraph.luke, "starring", 0.25), pageRankEdgeSchema),
          new GenericRowWithSchema(Array(dgraph.luke, "Person", "Luke Skywalker", null, null, null, null, 1.3047808f), pageRankVertexSchema),
        ), pageRankSchema),
        new GenericRowWithSchema(Array(
          new GenericRowWithSchema(Array(dgraph.sw3, "Film", null, Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, "Star Wars: Episode VI - Return of the Jedi", 0.7968128f), pageRankVertexSchema),
          new GenericRowWithSchema(Array(dgraph.sw3, dgraph.richard, "director", 0.25), pageRankEdgeSchema),
          new GenericRowWithSchema(Array(dgraph.richard, "Person", "Richard Marquand", null, null, null, null, 0.96613544f), pageRankVertexSchema),
        ), pageRankSchema)
      ).sortBy(row => (row.getAs[Row](0).getLong(0), row.getAs[Row](2).getLong(0)))
      assert(triplets === expected)
      assert(triplets.head.schema === expected.head.schema)
    }

    def doVerticesTest(load: () => DataFrame): Unit = {
      val vertices = removeDgraphVertices(load()).collect().sortBy(_.getLong(0))
      val expected = Seq(
        new GenericRowWithSchema(Array(dgraph.st1, "Film", null, Timestamp.valueOf("1979-12-07 00:00:00.0"), 1.39E8, 132, null), vertexSchema),
        new GenericRowWithSchema(Array(dgraph.leia, "Person", "Princess Leia", null, null, null, null), vertexSchema),
        new GenericRowWithSchema(Array(dgraph.lucas, "Person", "George Lucas", null, null, null, null), vertexSchema),
        new GenericRowWithSchema(Array(dgraph.irvin, "Person", "Irvin Kernshner", null, null, null, null), vertexSchema),
        new GenericRowWithSchema(Array(dgraph.sw1, "Film", null, Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, "Star Wars: Episode IV - A New Hope"), vertexSchema),
        new GenericRowWithSchema(Array(dgraph.sw2, "Film", null, Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, "Star Wars: Episode V - The Empire Strikes Back"), vertexSchema),
        new GenericRowWithSchema(Array(dgraph.luke, "Person", "Luke Skywalker", null, null, null, null), vertexSchema),
        new GenericRowWithSchema(Array(dgraph.han, "Person", "Han Solo", null, null, null, null), vertexSchema),
        new GenericRowWithSchema(Array(dgraph.richard, "Person", "Richard Marquand", null, null, null, null), vertexSchema),
        new GenericRowWithSchema(Array(dgraph.sw3, "Film", null, Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, "Star Wars: Episode VI - Return of the Jedi"), vertexSchema)
      ).sortBy(_.getLong(0))
      assert(vertices === expected)
      assert(vertices.head.schema === expected.head.schema)
    }

    def doEdgesTest(load: () => DataFrame): Unit = {
      val dgraphNodes = reader.dgraph.vertices(dgraph.target).where(isDgraphNode).select($"id").as[Long].collect().toSet
      val edges = removeDgraphEdges(load(), dgraphNodes).collect().sortBy(e => (e.getLong(0), e.getLong(1)))
      val expected = Seq(
        new GenericRowWithSchema(Array(dgraph.sw1, dgraph.leia, "starring"), edgeSchema),
        new GenericRowWithSchema(Array(dgraph.sw1, dgraph.lucas, "director"), edgeSchema),
        new GenericRowWithSchema(Array(dgraph.sw1, dgraph.luke, "starring"), edgeSchema),
        new GenericRowWithSchema(Array(dgraph.sw1, dgraph.han, "starring"), edgeSchema),
        new GenericRowWithSchema(Array(dgraph.sw2, dgraph.leia, "starring"), edgeSchema),
        new GenericRowWithSchema(Array(dgraph.sw2, dgraph.irvin, "director"), edgeSchema),
        new GenericRowWithSchema(Array(dgraph.sw2, dgraph.luke, "starring"), edgeSchema),
        new GenericRowWithSchema(Array(dgraph.sw2, dgraph.han, "starring"), edgeSchema),
        new GenericRowWithSchema(Array(dgraph.sw3, dgraph.leia, "starring"), edgeSchema),
        new GenericRowWithSchema(Array(dgraph.sw3, dgraph.luke, "starring"), edgeSchema),
        new GenericRowWithSchema(Array(dgraph.sw3, dgraph.han, "starring"), edgeSchema),
        new GenericRowWithSchema(Array(dgraph.sw3, dgraph.richard, "director"), edgeSchema)
      ).sortBy(e => (e.getLong(0), e.getLong(1)))
      assert(edges === expected)
      assert(edges.head.schema === expected.head.schema)
    }

    Seq(
      ("target", () => Seq(dgraph.target)),
      ("targets", () => Seq(dgraph.target, dgraph.targetLocalIp))
    ).foreach{case (test, targets) =>

      it(s"should load dgraph from $test via implicit session") {
        doGraphTest(() => loadGraph(reader, targets(): _*))
      }

      it(s"should load dgraph from $test via reader") {
        doGraphTest(() => reader.dgraph.graphframes(targets(): _*))
      }

      it(s"should load vertices from $test via implicit session") {
        doVerticesTest(() => loadVertices(reader, targets(): _*))
      }

      it(s"should load vertices from $test via reader") {
        doVerticesTest(() => reader.dgraph.vertices(targets(): _*))
      }

      it(s"should load edges from $test via implicit session") {
        doEdgesTest(() => loadEdges(reader, targets(): _*))
      }

      it(s"should load edges from $test via reader") {
        doEdgesTest(() => reader.dgraph.edges(targets(): _*))
      }

    }

  }

}
