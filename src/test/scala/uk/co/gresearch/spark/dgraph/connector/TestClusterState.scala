package uk.co.gresearch.spark.dgraph.connector

import java.util.UUID

import org.scalatest.FunSpec

class TestClusterState extends FunSpec {

  describe("ClusterState") {
    it("should load from Json") {
      val json =
        """{
          |  "counter": "21",
          |  "groups": {
          |    "1": {
          |      "members": {
          |        "1": {
          |          "id": "1",
          |          "groupId": 1,
          |          "addr": "localhost:7080",
          |          "leader": true,
          |          "lastUpdate": "1590831162"
          |        },
          |        "2": {
          |          "id": "2",
          |          "groupId": 1,
          |          "addr": "localhost:7081",
          |          "lastUpdate": "1590831163"
          |        }
          |      },
          |      "tablets": {
          |        "dgraph.graphql.schema": {
          |          "groupId": 1,
          |          "predicate": "dgraph.graphql.schema"
          |        },
          |        "dgraph.type": {
          |          "groupId": 1,
          |          "predicate": "dgraph.type"
          |        },
          |        "director": {
          |          "groupId": 1,
          |          "predicate": "director"
          |        }
          |      },
          |      "checksum": "2020581788589962106"
          |    },
          |    "2": {
          |      "members": {
          |        "1": {
          |          "id": "1",
          |          "groupId": 2,
          |          "addr": "127.0.0.1:7080",
          |          "leader": true,
          |          "lastUpdate": "1590831164"
          |        },
          |        "2": {
          |          "id": "2",
          |          "groupId": 2,
          |          "addr": "127.0.0.1:7081",
          |          "lastUpdate": "1590831165"
          |        }
          |      },
          |      "tablets": {
          |        "name": {
          |          "groupId": 2,
          |          "predicate": "name"
          |        },
          |        "release_date": {
          |          "groupId": 2,
          |          "predicate": "release_date"
          |        },
          |        "revenue": {
          |          "groupId": 2,
          |          "predicate": "revenue"
          |        }
          |      },
          |      "checksum": "2020581788589962107"
          |    }
          |  },
          |  "zeros": {
          |    "1": {
          |      "id": "1",
          |      "addr": "localhost:5080",
          |      "leader": true
          |    }
          |  },
          |  "maxLeaseId": "10000",
          |  "maxTxnTs": "20000",
          |  "maxRaftId": "1",
          |  "cid": "5aacce50-a95f-440b-a32e-fbe6b4003980",
          |  "license": {
          |    "maxNodes": "18446744073709551615",
          |    "expiryTs": "1593255848",
          |    "enabled": true
          |  }
          |}
          |""".stripMargin

      val state = ClusterState.fromJson(json)

      assert(state.groupMembers === Map(
        "1" -> Set(Target("localhost:7080"), Target("localhost:7081")),
        "2" -> Set(Target("127.0.0.1:7080"), Target("127.0.0.1:7081"))
      ))
      assert(state.groupPredicates === Map(
        "1" -> Set("dgraph.graphql.schema", "dgraph.type", "director"),
        "2" -> Set("name", "release_date", "revenue")
      ))
      assert(state.maxLeaseId === 10000)
      assert(state.cid === UUID.fromString("5aacce50-a95f-440b-a32e-fbe6b4003980"))
    }
  }
}
