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

package uk.co.gresearch.spark.dgraph.connector

import java.sql.Timestamp

import org.scalatest.FunSpec

class TestTriplesFactory extends FunSpec {

  describe("TriplesFactory") {
    it("should parse JSON response") {
      val schema = Schema(Set(
        Predicate("release_date", "datetime"),
        Predicate("revenue", "int"),
        Predicate("running_time", "int"),
        Predicate("director", "uid"),
        Predicate("starring", "uid")
      ))

      val json =
        """
          |{
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

      val triplesFactory = TriplesFactory(schema)
      val triples = triplesFactory.fromJson(json, "result").toList
      assert(triples === Seq(
        Triple(Uid(1), "name", "Star Wars: Episode IV - A New Hope"),
        Triple(Uid(1), "release_date", Timestamp.valueOf("1977-05-25 00:00:00")),
        Triple(Uid(1), "revenue", 775000000L),
        Triple(Uid(1), "running_time", 121L),
        Triple(Uid(1), "starring", Uid(2)),
        Triple(Uid(1), "starring", Uid(3)),
        Triple(Uid(1), "starring", Uid(7)),
        Triple(Uid(1), "director", Uid(4)),
        Triple(Uid(2), "name", "Luke Skywalker"),
        Triple(Uid(3), "name", "Han Solo"),
        Triple(Uid(4), "name", "George Lucas"),
        Triple(Uid(5), "name", "Irvin Kernshner"),
        Triple(Uid(6), "name", "Richard Marquand"),
        Triple(Uid(7), "name", "Princess Leia"),
        Triple(Uid(8), "name", "Star Wars: Episode V - The Empire Strikes Back"),
        Triple(Uid(8), "release_date", Timestamp.valueOf("1980-05-21 00:00:00")),
        Triple(Uid(8), "revenue", 534000000L),
        Triple(Uid(8), "running_time", 124L),
        Triple(Uid(8), "starring", Uid(2)),
        Triple(Uid(8), "starring", Uid(3)),
        Triple(Uid(8), "starring", Uid(7)),
        Triple(Uid(8), "director", Uid(5)),
        Triple(Uid(9), "name", "Star Wars: Episode VI - Return of the Jedi"),
        Triple(Uid(9), "release_date", Timestamp.valueOf("1983-05-25 00:00:00")),
        Triple(Uid(9), "revenue", 572000000L),
        Triple(Uid(9), "running_time", 131L),
        Triple(Uid(9), "starring", Uid(2)),
        Triple(Uid(9), "starring", Uid(3)),
        Triple(Uid(9), "starring", Uid(7)),
        Triple(Uid(9), "director", Uid(6)),
        Triple(Uid(10), "name", "Star Trek: The Motion Picture"),
        Triple(Uid(10), "release_date", Timestamp.valueOf("1979-12-07 00:00:00")),
        Triple(Uid(10), "revenue", 139000000L),
        Triple(Uid(10), "running_time", 132L),
      ))
    }
  }
}
