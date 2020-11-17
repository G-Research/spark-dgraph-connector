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

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, FloatType, LongType, StructField, StructType}
import org.graphframes.GraphFrame
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster

class TestGraphFrames extends AnyFunSpec
  with SparkTestSession with DgraphTestCluster {

  describe("GraphFrames") {

    def doGraphTest(load: () => GraphFrame): Unit = {
      val graph = load()
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
      ).collect().toSet

      val expected = Set(
        Row(
          Row(dgraph.sw1, null, null, "Film", null, Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, "Star Wars: Episode IV - A New Hope", 0.8118081180811808.toFloat),
          Row(dgraph.sw1, dgraph.leia, "starring", 0.25),
          Row(dgraph.leia, null, null, "Person", "Princess Leia", null, null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(dgraph.sw2, null, null, "Film", null, Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, "Star Wars: Episode V - The Empire Strikes Back", 0.8118081180811808.toFloat),
          Row(dgraph.sw2, dgraph.leia, "starring", 0.25),
          Row(dgraph.leia, null, null, "Person", "Princess Leia", null, null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(dgraph.sw3, null, null, "Film", null, Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, "Star Wars: Episode VI - Return of the Jedi", 0.8118081180811808.toFloat),
          Row(dgraph.sw3, dgraph.leia, "starring", 0.25),
          Row(dgraph.leia, null, null, "Person", "Princess Leia", null, null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(dgraph.sw2, null, null, "Film", null, Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, "Star Wars: Episode V - The Empire Strikes Back", 0.8118081180811808.toFloat),
          Row(dgraph.sw2, dgraph.irvin, "director", 0.25),
          Row(dgraph.irvin, null, null, "Person", "Irvin Kernshner", null, null, null, null, 0.9843173431734319.toFloat),
        ),
        Row(
          Row(dgraph.sw1, null, null, "Film", null, Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, "Star Wars: Episode IV - A New Hope", 0.8118081180811808.toFloat),
          Row(dgraph.sw1, dgraph.han, "starring", 0.25),
          Row(dgraph.han, null, null, "Person", "Han Solo", null, null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(dgraph.sw2, null, null, "Film", null, Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, "Star Wars: Episode V - The Empire Strikes Back", 0.8118081180811808.toFloat),
          Row(dgraph.sw2, dgraph.han, "starring", 0.25),
          Row(dgraph.han, null, null, "Person", "Han Solo", null, null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(dgraph.sw3, null, null, "Film", null, Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, "Star Wars: Episode VI - Return of the Jedi", 0.8118081180811808.toFloat),
          Row(dgraph.sw3, dgraph.han, "starring", 0.25),
          Row(dgraph.han, null, null, "Person", "Han Solo", null, null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(dgraph.sw1, null, null, "Film", null, Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, "Star Wars: Episode IV - A New Hope", 0.8118081180811808.toFloat),
          Row(dgraph.sw1, dgraph.lucas, "director", 0.25),
          Row(dgraph.lucas, null, null, "Person", "George Lucas", null, null, null, null, 0.9843173431734319.toFloat),
        ),
        Row(
          Row(dgraph.sw1, null, null, "Film", null, Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, "Star Wars: Episode IV - A New Hope", 0.8118081180811808.toFloat),
          Row(dgraph.sw1, dgraph.luke, "starring", 0.25),
          Row(dgraph.luke, null, null, "Person", "Luke Skywalker", null, null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(dgraph.sw2, null, null, "Film", null, Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, "Star Wars: Episode V - The Empire Strikes Back", 0.8118081180811808.toFloat),
          Row(dgraph.sw2, dgraph.luke, "starring", 0.25),
          Row(dgraph.luke, null, null, "Person", "Luke Skywalker", null, null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(dgraph.sw3, null, null, "Film", null, Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, "Star Wars: Episode VI - Return of the Jedi", 0.8118081180811808.toFloat),
          Row(dgraph.sw3, dgraph.luke, "starring", 0.25),
          Row(dgraph.luke, null, null, "Person", "Luke Skywalker", null, null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(dgraph.sw3, null, null, "Film", null, Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, "Star Wars: Episode VI - Return of the Jedi", 0.8118081180811808.toFloat),
          Row(dgraph.sw3, dgraph.richard, "director", 0.25),
          Row(dgraph.richard, null, null, "Person", "Richard Marquand", null, null, null, null, 0.9843173431734319.toFloat),
        )
      )
      assert(triplets === expected)
    }

    def doVerticesTest(load: () => DataFrame): Unit = {
      val vertices = load().collect().toSet
      val expected = Set(
        Row(dgraph.graphQlSchema, "", "dgraph.graphql.schema", "dgraph.graphql", null, null, null, null, null),
        Row(dgraph.st1, null, null, "Film", null, Timestamp.valueOf("1979-12-07 00:00:00.0"), 1.39E8, 132, null),
        Row(dgraph.leia, null, null, "Person", "Princess Leia", null, null, null, null),
        Row(dgraph.lucas, null, null, "Person", "George Lucas", null, null, null, null),
        Row(dgraph.irvin, null, null, "Person", "Irvin Kernshner", null, null, null, null),
        Row(dgraph.sw1, null, null, "Film", null, Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, "Star Wars: Episode IV - A New Hope"),
        Row(dgraph.sw2, null, null, "Film", null, Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, "Star Wars: Episode V - The Empire Strikes Back"),
        Row(dgraph.luke, null, null, "Person", "Luke Skywalker", null, null, null, null),
        Row(dgraph.han, null, null, "Person", "Han Solo", null, null, null, null),
        Row(dgraph.richard, null, null, "Person", "Richard Marquand", null, null, null, null),
        Row(dgraph.sw3, null, null, "Film", null, Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, "Star Wars: Episode VI - Return of the Jedi")
      )
      assert(vertices === expected)
    }

    def doEdgesTest(load: () => DataFrame): Unit = {
      val edges = load().collect().toSet
      val expected = Set(
        Row(dgraph.sw1, dgraph.leia, "starring"),
        Row(dgraph.sw1, dgraph.lucas, "director"),
        Row(dgraph.sw1, dgraph.luke, "starring"),
        Row(dgraph.sw1, dgraph.han, "starring"),
        Row(dgraph.sw2, dgraph.leia, "starring"),
        Row(dgraph.sw2, dgraph.irvin, "director"),
        Row(dgraph.sw2, dgraph.luke, "starring"),
        Row(dgraph.sw2, dgraph.han, "starring"),
        Row(dgraph.sw3, dgraph.leia, "starring"),
        Row(dgraph.sw3, dgraph.luke, "starring"),
        Row(dgraph.sw3, dgraph.han, "starring"),
        Row(dgraph.sw3, dgraph.richard, "director")
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
        doGraphTest(() => spark.read.dgraph.graphframes(targets(): _*))
      }

      it(s"should load vertices from $test via implicit session") {
        doVerticesTest(() => loadVertices(targets(): _*))
      }

      it(s"should load vertices from $test via reader") {
        doVerticesTest(() => spark.read.dgraph.vertices(targets(): _*))
      }

      it(s"should load edges from $test via implicit session") {
        doEdgesTest(() => loadEdges(targets(): _*))
      }

      it(s"should load edges from $test via reader") {
        doEdgesTest(() => spark.read.dgraph.edges(targets(): _*))
      }

    }

  }

}
