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

package uk.co.gresearch.spark.dgraph.connector

import java.util.UUID

import org.scalatest.funspec.AnyFunSpec

class TestClusterState extends AnyFunSpec {

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
          |        "dgraph.graphql.xid": {
          |          "groupId": 1,
          |          "predicate": "dgraph.graphql.xid"
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

      val state = ClusterState.fromJson(Json(json))

      assert(state.groupMembers === Map(
        "1" -> Set(Target("localhost:9080"), Target("localhost:9081")),
        "2" -> Set(Target("127.0.0.1:9080"), Target("127.0.0.1:9081"))
      ))
      assert(state.groupPredicates === Map(
        "1" -> Set("dgraph.graphql.schema", "dgraph.graphql.xid", "dgraph.type", "director"),
        "2" -> Set("name", "release_date", "revenue")
      ))
      assert(state.maxLeaseId === 10000)
      assert(state.cid === UUID.fromString("5aacce50-a95f-440b-a32e-fbe6b4003980"))
    }

    it("should ignore null characters") {
      val json =
        """{
          |  "counter": "25",
          |  "groups": {
          |    "1": {
          |      "members": {
          |        "1": {
          |          "id": "1",
          |          "groupId": 1,
          |          "addr": "localhost:7081",
          |          "leader": true,
          |          "amDead": false,
          |          "lastUpdate": "1619348857",
          |          "learner": false,
          |          "clusterInfoOnly": false,
          |          "forceGroupId": false
          |        },
          |        "2": {
          |          "id": "2",
          |          "groupId": 1,
          |          "addr": "localhost:7082",
          |          "leader": true,
          |          "amDead": false,
          |          "lastUpdate": "1619348857",
          |          "learner": false,
          |          "clusterInfoOnly": false,
          |          "forceGroupId": false
          |        }
          |      },
          |      "tablets": {
          |        "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000dgraph.graphql.schema": {
          |          "groupId": 1,
          |          "predicate": "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000dgraph.graphql.schema",
          |          "force": false,
          |          "onDiskBytes": "0",
          |          "remove": false,
          |          "readOnly": false,
          |          "moveTs": "0",
          |          "uncompressedBytes": "0"
          |        },
          |        "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000dgraph.type": {
          |          "groupId": 1,
          |          "predicate": "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000dgraph.type",
          |          "force": false,
          |          "onDiskBytes": "0",
          |          "remove": false,
          |          "readOnly": false,
          |          "moveTs": "0",
          |          "uncompressedBytes": "0"
          |        }
          |      },
          |      "snapshotTs": "8",
          |      "checksum": "7101184639612340275",
          |      "checkpointTs": "0"
          |    },
          |    "2": {
          |      "members": {
          |        "1": {
          |          "id": "1",
          |          "groupId": 2,
          |          "addr": "127.0.0.1:7081",
          |          "leader": true,
          |          "amDead": false,
          |          "lastUpdate": "1619348857",
          |          "learner": false,
          |          "clusterInfoOnly": false,
          |          "forceGroupId": false
          |        },
          |        "2": {
          |          "id": "2",
          |          "groupId": 2,
          |          "addr": "127.0.0.1:7082",
          |          "leader": true,
          |          "amDead": false,
          |          "lastUpdate": "1619348857",
          |          "learner": false,
          |          "clusterInfoOnly": false,
          |          "forceGroupId": false
          |        }
          |      },
          |      "tablets": {
          |        "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000director": {
          |          "groupId": 2,
          |          "predicate": "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000director",
          |          "force": false,
          |          "onDiskBytes": "0",
          |          "remove": false,
          |          "readOnly": false,
          |          "moveTs": "0",
          |          "uncompressedBytes": "0"
          |        },
          |        "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000name": {
          |          "groupId": 2,
          |          "predicate": "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000name",
          |          "force": false,
          |          "onDiskBytes": "0",
          |          "remove": false,
          |          "readOnly": false,
          |          "moveTs": "0",
          |          "uncompressedBytes": "0"
          |        }
          |      },
          |      "snapshotTs": "8",
          |      "checksum": "7101184639612340275",
          |      "checkpointTs": "0"
          |    }
          |  },
          |  "zeros": {
          |    "1": {
          |      "id": "1",
          |      "groupId": 0,
          |      "addr": "localhost:5081",
          |      "leader": true,
          |      "amDead": false,
          |      "lastUpdate": "0",
          |      "learner": false,
          |      "clusterInfoOnly": false,
          |      "forceGroupId": false
          |    }
          |  },
          |  "maxUID": "10000",
          |  "maxTxnTs": "10000",
          |  "maxNsID": "0",
          |  "maxRaftId": "1",
          |  "removed": [],
          |  "cid": "350fd4f5-771d-4021-8ef9-cd1b79aa6ea0",
          |  "license": {
          |    "user": "",
          |    "maxNodes": "18446744073709551615",
          |    "expiryTs": "1621940859",
          |    "enabled": true
          |  }
          |}""".stripMargin

      val state = ClusterState.fromJson(Json(json))

      assert(state.groupMembers === Map(
        "1" -> Set(Target("localhost:9081"), Target("localhost:9082")),
        "2" -> Set(Target("127.0.0.1:9081"), Target("127.0.0.1:9082"))
      ))
      assert(state.groupPredicates === Map(
        "1" -> Set("dgraph.graphql.schema", "dgraph.type"),
        "2" -> Set("director", "name")
      ))
      assert(state.maxLeaseId === 10000)
      assert(state.cid === UUID.fromString("350fd4f5-771d-4021-8ef9-cd1b79aa6ea0"))
    }
  }
}
