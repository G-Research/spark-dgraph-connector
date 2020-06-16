package uk.co.gresearch.spark.dgraph.graphframes

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.graphframes.GraphFrame
import org.scalatest.FunSpec
import uk.co.gresearch.spark.SparkTestSession
import uk.co.gresearch.spark.dgraph.DgraphTestCluster

class TestGraphFrames extends FunSpec
  with SparkTestSession with DgraphTestCluster {

  describe("GraphFrames") {

    def doGraphTest(load: () => GraphFrame): Unit = {
      val graph = load()
      val pageRank = graph.pageRank.maxIter(10)
      assert(pageRank.run().triplets.collect() === Seq(
        Row(
          Row(5, null, "Film", "Star Wars: Episode IV - A New Hope", Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, 0.7968127490039841),
          Row(5, 2, "starring", 0.25),
          Row(2, null, "Person", "Princess Leia", null, null, null, 1.3047808764940239),
        ),
        Row(
          Row(6, null, "Film", "Star Wars: Episode V - The Empire Strikes Back", Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, 0.7968127490039841),
          Row(6, 2, "starring", 0.25),
          Row(2, null, "Person", "Princess Leia", null, null, null, 1.3047808764940239),
        ),
        Row(
          Row(10, null, "Film", "Star Wars: Episode VI - Return of the Jedi", Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, 0.7968127490039841),
          Row(10, 2, "starring", 0.25),
          Row(2, null, "Person", "Princess Leia", null, null, null, 1.3047808764940239),
        ),
        Row(
          Row(6, null, "Film", "Star Wars: Episode V - The Empire Strikes Back", Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, 0.7968127490039841),
          Row(6, 4, "director", 0.25),
          Row(4, null, "Person", "Irvin Kernshner", null, null, null, 0.9661354581673308),
        ),
        Row(
          Row(5, null, "Film", "Star Wars: Episode IV - A New Hope", Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, 0.7968127490039841),
          Row(5, 8, "starring", 0.25),
          Row(8, null, "Person", "Han Solo", null, null, null, 1.3047808764940239),
        ),
        Row(
          Row(6, null, "Film", "Star Wars: Episode V - The Empire Strikes Back", Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, 0.7968127490039841),
          Row(6, 8, "starring", 0.25),
          Row(8, null, "Person", "Han Solo", null, null, null, 1.3047808764940239),
        ),
        Row(
          Row(10, null, "Film", "Star Wars: Episode VI - Return of the Jedi", Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, 0.7968127490039841),
          Row(10, 8, "starring", 0.25),
          Row(8, null, "Person", "Han Solo", null, null, null, 1.3047808764940239),
        ),
        Row(
          Row(5, null, "Film", "Star Wars: Episode IV - A New Hope", Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, 0.7968127490039841),
          Row(5, 3, "director", 0.25),
          Row(3, null, "Person", "George Lucas", null, null, null, 0.9661354581673308),
        ),
        Row(
          Row(5, null, "Film", "Star Wars: Episode IV - A New Hope", Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, 0.7968127490039841),
          Row(5, 7, "starring", 0.25),
          Row(7, null, "Person", "Luke Skywalker", null, null, null, 1.3047808764940239),
        ),
        Row(
          Row(6, null, "Film", "Star Wars: Episode V - The Empire Strikes Back", Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, 0.7968127490039841),
          Row(6, 7, "starring", 0.25),
          Row(7, null, "Person", "Luke Skywalker", null, null, null, 1.3047808764940239),
        ),
        Row(
          Row(10, null, "Film", "Star Wars: Episode VI - Return of the Jedi", Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, 0.7968127490039841),
          Row(10, 7, "starring", 0.25),
          Row(7, null, "Person", "Luke Skywalker", null, null, null, 1.3047808764940239),
        ),
        Row(
          Row(10, null, "Film", "Star Wars: Episode VI - Return of the Jedi", Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, 0.7968127490039841),
          Row(10, 9, "director", 0.25),
          Row(9, null, "Person", "Richard Marquand", null, null, null, 0.9661354581673308),
        )
      ))
    }

    def doVerticesTest(load: () => DataFrame): Unit = {
      val vertices = load()
      assert(vertices.collect() === Seq(
        Row(1, null, "Film", "Star Trek: The Motion Picture", Timestamp.valueOf("1979-12-07 00:00:00.0"), 1.39E8, 132),
        Row(2, null, "Person", "Princess Leia", null, null, null),
        Row(3, null, "Person", "George Lucas", null, null, null),
        Row(4, null, "Person", "Irvin Kernshner", null, null, null),
        Row(5, null, "Film", "Star Wars: Episode IV - A New Hope", Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121),
        Row(6, null, "Film", "Star Wars: Episode V - The Empire Strikes Back", Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124),
        Row(7, null, "Person", "Luke Skywalker", null, null, null),
        Row(8, null, "Person", "Han Solo", null, null, null),
        Row(9, null, "Person", "Richard Marquand", null, null, null),
        Row(10, null, "Film", "Star Wars: Episode VI - Return of the Jedi", Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131)
      ))
    }

    def doEdgesTest(load: () => DataFrame): Unit = {
      val edges = load()
      val sorted = edges.coalesce(1).sortWithinPartitions(edges.columns.map(col): _*)
      assert(sorted.collect() === Seq(
        Row(5, 2, "starring"),
        Row(5, 3, "director"),
        Row(5, 7, "starring"),
        Row(5, 8, "starring"),
        Row(6, 2, "starring"),
        Row(6, 4, "director"),
        Row(6, 7, "starring"),
        Row(6, 8, "starring"),
        Row(10, 2, "starring"),
        Row(10, 7, "starring"),
        Row(10, 8, "starring"),
        Row(10, 9, "director")
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
        doVerticesTest(() => loadVertices(targets: _*))
      }

      it(s"should load vertices from $test via reader") {
        doVerticesTest(() => spark.read.dgraphVertices(targets: _*))
      }

      it(s"should load edges from $test via implicit session") {
        doEdgesTest(() => loadEdges(targets: _*))
      }

      it(s"should load edges from $test via reader") {
        doEdgesTest(() => spark.read.dgraphEdges(targets: _*))
      }

    }

  }

}
