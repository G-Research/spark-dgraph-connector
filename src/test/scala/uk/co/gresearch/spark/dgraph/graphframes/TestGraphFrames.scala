package uk.co.gresearch.spark.dgraph.graphframes

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, FloatType, LongType, StructField, StructType}
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
          Row(sw1, null, null, "Film", "Star Wars: Episode IV - A New Hope", Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, 0.8118081180811808.toFloat),
          Row(sw1, leia, "starring", 0.25),
          Row(leia, null, null, "Person", "Princess Leia", null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(sw2, null, null, "Film", "Star Wars: Episode V - The Empire Strikes Back", Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, 0.8118081180811808.toFloat),
          Row(sw2, leia, "starring", 0.25),
          Row(leia, null, null, "Person", "Princess Leia", null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(sw3, null, null, "Film", "Star Wars: Episode VI - Return of the Jedi", Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, 0.8118081180811808.toFloat),
          Row(sw3, leia, "starring", 0.25),
          Row(leia, null, null, "Person", "Princess Leia", null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(sw2, null, null, "Film", "Star Wars: Episode V - The Empire Strikes Back", Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, 0.8118081180811808.toFloat),
          Row(sw2, irvin, "director", 0.25),
          Row(irvin, null, null, "Person", "Irvin Kernshner", null, null, null, 0.9843173431734319.toFloat),
        ),
        Row(
          Row(sw1, null, null, "Film", "Star Wars: Episode IV - A New Hope", Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, 0.8118081180811808.toFloat),
          Row(sw1, han, "starring", 0.25),
          Row(han, null, null, "Person", "Han Solo", null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(sw2, null, null, "Film", "Star Wars: Episode V - The Empire Strikes Back", Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, 0.8118081180811808.toFloat),
          Row(sw2, han, "starring", 0.25),
          Row(han, null, null, "Person", "Han Solo", null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(sw3, null, null, "Film", "Star Wars: Episode VI - Return of the Jedi", Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, 0.8118081180811808.toFloat),
          Row(sw3, han, "starring", 0.25),
          Row(han, null, null, "Person", "Han Solo", null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(sw1, null, null, "Film", "Star Wars: Episode IV - A New Hope", Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, 0.8118081180811808.toFloat),
          Row(sw1, lucas, "director", 0.25),
          Row(lucas, null, null, "Person", "George Lucas", null, null, null, 0.9843173431734319.toFloat),
        ),
        Row(
          Row(sw1, null, null, "Film", "Star Wars: Episode IV - A New Hope", Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121, 0.8118081180811808.toFloat),
          Row(sw1, luke, "starring", 0.25),
          Row(luke, null, null, "Person", "Luke Skywalker", null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(sw2, null, null, "Film", "Star Wars: Episode V - The Empire Strikes Back", Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124, 0.8118081180811808.toFloat),
          Row(sw2, luke, "starring", 0.25),
          Row(luke, null, null, "Person", "Luke Skywalker", null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(sw3, null, null, "Film", "Star Wars: Episode VI - Return of the Jedi", Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, 0.8118081180811808.toFloat),
          Row(sw3, luke, "starring", 0.25),
          Row(luke, null, null, "Person", "Luke Skywalker", null, null, null, 1.3293357933579335.toFloat),
        ),
        Row(
          Row(sw3, null, null, "Film", "Star Wars: Episode VI - Return of the Jedi", Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131, 0.8118081180811808.toFloat),
          Row(sw3, richard, "director", 0.25),
          Row(richard, null, null, "Person", "Richard Marquand", null, null, null, 0.9843173431734319.toFloat),
        )
      )
      doubleTriples.show(100, false)
      assert(triplets === expected)
    }

    def doVerticesTest(load: () => DataFrame): Unit = {
      load().printSchema()
      load().show(false)
      val vertices = load().collect().toSet
      val expected = Set(
        Row(1, "", "dgraph.graphql.schema", "dgraph.graphql", null, null, null, null),
        Row(st1, null, null, "Film", "Star Trek: The Motion Picture", Timestamp.valueOf("1979-12-07 00:00:00.0"), 1.39E8, 132),
        Row(leia, null, null, "Person", "Princess Leia", null, null, null),
        Row(lucas, null, null, "Person", "George Lucas", null, null, null),
        Row(irvin, null, null, "Person", "Irvin Kernshner", null, null, null),
        Row(sw1, null, null, "Film", "Star Wars: Episode IV - A New Hope", Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121),
        Row(sw2, null, null, "Film", "Star Wars: Episode V - The Empire Strikes Back", Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124),
        Row(luke, null, null, "Person", "Luke Skywalker", null, null, null),
        Row(han, null, null, "Person", "Han Solo", null, null, null),
        Row(richard, null, null, "Person", "Richard Marquand", null, null, null),
        Row(sw3, null, null, "Film", "Star Wars: Episode VI - Return of the Jedi", Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131)
      )
      assert(vertices === expected)
    }

    def doEdgesTest(load: () => DataFrame): Unit = {
      val edges = load().collect().toSet
      val expected = Set(
        Row(sw1, leia, "starring"),
        Row(sw1, lucas, "director"),
        Row(sw1, luke, "starring"),
        Row(sw1, han, "starring"),
        Row(sw2, leia, "starring"),
        Row(sw2, irvin, "director"),
        Row(sw2, luke, "starring"),
        Row(sw2, han, "starring"),
        Row(sw3, leia, "starring"),
        Row(sw3, luke, "starring"),
        Row(sw3, han, "starring"),
        Row(sw3, richard, "director")
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
        doVerticesTest(() => loadVertices(targets(): _*))
      }

      it(s"should load vertices from $test via reader") {
        doVerticesTest(() => spark.read.dgraphVertices(targets(): _*))
      }

      it(s"should load edges from $test via implicit session") {
        doEdgesTest(() => loadEdges(targets(): _*))
      }

      it(s"should load edges from $test via reader") {
        doEdgesTest(() => spark.read.dgraphEdges(targets(): _*))
      }

    }

  }

}
