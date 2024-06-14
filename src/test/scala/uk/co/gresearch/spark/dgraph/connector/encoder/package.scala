package uk.co.gresearch.spark.dgraph.connector

package object encoder {
  val schema: Schema = Schema(
    Set(
      Predicate("name", "string"),
      Predicate("release_date", "datetime"),
      Predicate("revenue", "float"),
      Predicate("running_time", "int"),
      Predicate("director", "uid"),
      Predicate("starring", "uid")
    )
  )

  val json: String =
    """{
      |    "result": [
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

  val jsonWithLargeUids: String =
    """{
      |    "result": [
      |      {
      |        "uid": "0xa7eb7890d6db0f1f",
      |        "name": "Star Wars: Episode IV - A New Hope",
      |        "release_date": "1977-05-25T00:00:00Z",
      |        "revenue": "775000000",
      |        "running_time": 121,
      |        "starring": [
      |          {
      |            "uid": "0x2b0742de9736084f"
      |          },
      |          {
      |            "uid": "0xdde13018fc0c2ac"
      |          },
      |          {
      |            "uid": "0xe5f157883987549f"
      |          }
      |        ],
      |        "director": [
      |          {
      |            "uid": "0x71ff21075549faf8"
      |          }
      |        ]
      |      }
      |    ]
      |  }""".stripMargin
}
