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

import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession

class TestGraphX extends FunSpec with SparkTestSession {

  describe("GraphX") {

    Seq(
      ("target", Seq("localhost:9080")),
      ("targets", Seq("localhost:9080", "127.0.0.1:9080"))
    ).foreach{case (test, targets) =>

      it(s"should load dgraph from $test via implicit session") {
        val graph = loadGraph(targets: _*)
        val pageRank = graph.pageRank(0.0001)
        pageRank.vertices.foreach(println)
      }

      it(s"should load dgraph from $test via reader") {
        val graph = spark.read.dgraph(targets: _*)
        val pageRank = graph.pageRank(0.0001)
        pageRank.vertices.foreach(println)
      }

      it(s"should load vertices from $test via implicit session") {
        val vertices = loadVertices(targets: _*)
        vertices.foreach(println)
      }

      it(s"should load vertices from $test via reader") {
        val vertices = spark.read.dgraphVertices(targets: _*)
        vertices.foreach(println)
      }

      it(s"should load edges from $test via implicit session") {
        val edges = loadEdges(targets: _*)
        edges.foreach(println)
      }

      it(s"should load edges from $test via reader") {
        val edges = spark.read.dgraphEdges(targets: _*)
        edges.foreach(println)
      }

    }

  }

}
