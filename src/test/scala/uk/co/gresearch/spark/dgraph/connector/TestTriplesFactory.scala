package uk.co.gresearch.spark.dgraph.connector

import org.scalatest.FunSpec

class TestTriplesFactory extends FunSpec {

  describe("TriplesFactory") {
    it("should parse JSON response") {
      val json =
        """
          |{
          |    "nodes": [
          |      {
          |        "uid": "0x1",
          |        "name": "Star Wars: Episode IV - A New Hope",
          |        "release_date": "1977-05-25T00:00:00Z",
          |        "revenue": "775000000",
          |        "running_time": 121,
          |        "starring": [
          |          {
          |            "uid": "0x2"
          |          },
          |          {
          |            "uid": "0x3"
          |          },
          |          {
          |            "uid": "0x7"
          |          }
          |        ],
          |        "director": [
          |          {
          |            "uid": "0x4"
          |          }
          |        ]
          |      },
          |      {
          |        "uid": "0x2",
          |        "name": "Luke Skywalker"
          |      },
          |      {
          |        "uid": "0x3",
          |        "name": "Han Solo"
          |      },
          |      {
          |        "uid": "0x4",
          |        "name": "George Lucas"
          |      },
          |      {
          |        "uid": "0x5",
          |        "name": "Irvin Kernshner"
          |      },
          |      {
          |        "uid": "0x6",
          |        "name": "Richard Marquand"
          |      },
          |      {
          |        "uid": "0x7",
          |        "name": "Princess Leia"
          |      },
          |      {
          |        "uid": "0x8",
          |        "name": "Star Wars: Episode V - The Empire Strikes Back",
          |        "release_date": "1980-05-21T00:00:00Z",
          |        "revenue": "534000000",
          |        "running_time": 124,
          |        "starring": [
          |          {
          |            "uid": "0x2"
          |          },
          |          {
          |            "uid": "0x3"
          |          },
          |          {
          |            "uid": "0x7"
          |          }
          |        ],
          |        "director": [
          |          {
          |            "uid": "0x5"
          |          }
          |        ]
          |      },
          |      {
          |        "uid": "0x9",
          |        "name": "Star Wars: Episode VI - Return of the Jedi",
          |        "release_date": "1983-05-25T00:00:00Z",
          |        "revenue": "572000000",
          |        "running_time": 131,
          |        "starring": [
          |          {
          |            "uid": "0x2"
          |          },
          |          {
          |            "uid": "0x3"
          |          },
          |          {
          |            "uid": "0x7"
          |          }
          |        ],
          |        "director": [
          |          {
          |            "uid": "0x6"
          |          }
          |        ]
          |      },
          |      {
          |        "uid": "0xa",
          |        "name": "Star Trek: The Motion Picture",
          |        "release_date": "1979-12-07T00:00:00Z",
          |        "revenue": "139000000",
          |        "running_time": 132
          |      }
          |    ]
          |  }""".stripMargin

      val triples = TriplesFactory.fromJson(json).toSeq
      assert(triples === Seq(
        Triple(1, "name", "Star Wars: Episode IV - A New Hope"),
        Triple(1, "release_date", "1977-05-25T00:00:00Z"),
        Triple(1, "revenue", "775000000"),
        Triple(1, "running_time", "121"),
        Triple(1, "starring", "2"),
        Triple(1, "starring", "3"),
        Triple(1, "starring", "7"),
        Triple(1, "director", "4"),
        Triple(2, "name", "Luke Skywalker"),
        Triple(3, "name", "Han Solo"),
        Triple(4, "name", "George Lucas"),
        Triple(5, "name", "Irvin Kernshner"),
        Triple(6, "name", "Richard Marquand"),
        Triple(7, "name", "Princess Leia"),
        Triple(8, "name", "Star Wars: Episode V - The Empire Strikes Back"),
        Triple(8, "release_date", "1980-05-21T00:00:00Z"),
        Triple(8, "revenue", "534000000"),
        Triple(8, "running_time", "124"),
        Triple(8, "starring", "2"),
        Triple(8, "starring", "3"),
        Triple(8, "starring", "7"),
        Triple(8, "director", "5"),
        Triple(9, "name", "Star Wars: Episode VI - Return of the Jedi"),
        Triple(9, "release_date", "1983-05-25T00:00:00Z"),
        Triple(9, "revenue", "572000000"),
        Triple(9, "running_time", "131"),
        Triple(9, "starring", "2"),
        Triple(9, "starring", "3"),
        Triple(9, "starring", "7"),
        Triple(9, "director", "6"),
        Triple(10, "name", "Star Trek: The Motion Picture"),
        Triple(10, "release_date", "1979-12-07T00:00:00Z"),
        Triple(10, "revenue", "139000000"),
        Triple(10, "running_time", "132"),
      ))
    }
  }
}
