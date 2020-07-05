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

package uk.co.gresearch.spark.dgraph

import java.util.UUID

import com.google.gson.{Gson, JsonArray, JsonObject}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.scalatest.{BeforeAndAfterAll, Suite}
import requests.{RequestBlob, Response}
import uk.co.gresearch.spark.dgraph.DgraphTestCluster.isDgraphClusterRunning
import uk.co.gresearch.spark.dgraph.connector.encoder.{JsonNodeInternalRowEncoder, NoColumnInfo, StringTripleEncoder}
import uk.co.gresearch.spark.dgraph.connector.executor.DgraphExecutor
import uk.co.gresearch.spark.dgraph.connector.{ClusterStateProvider, GraphQl, Target, Uid}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.sys.process.{Process, ProcessLogger}

trait DgraphTestCluster extends BeforeAndAfterAll { this: Suite =>

  val clusterVersion = "20.03.3"
  val clusterAlwaysStartUp = false  // ignores running cluster and starts a new if true
  val cluster: DgraphCluster = DgraphCluster(s"dgraph-unit-test-cluster-${UUID.randomUUID()}", clusterVersion)
  def clusterTarget: String = cluster.grpc

  val testClusterRunning: Boolean = isDgraphClusterRunning && (!DgraphTestCluster.isDockerInstalled || runningDockerDgraphCluster.isEmpty)
  if (clusterAlwaysStartUp || !testClusterRunning)
    assert(DgraphTestCluster.isDockerInstalled, "docker must be installed")

  lazy val graphQlSchema: Long = cluster.uids("dgraph.graphql.schema")
  lazy val han: Long = cluster.uids("han")
  lazy val irvin: Long = cluster.uids("irvin")
  lazy val leia: Long = cluster.uids("leia")
  lazy val lucas: Long = cluster.uids("lucas")
  lazy val luke: Long = cluster.uids("luke")
  lazy val richard: Long = cluster.uids("richard")
  lazy val st1: Long = cluster.uids("st1")
  lazy val sw1: Long = cluster.uids("sw1")
  lazy val sw2: Long = cluster.uids("sw2")
  lazy val sw3: Long = cluster.uids("sw3")
  lazy val allUids: Seq[Long] = Seq(graphQlSchema, han, irvin, leia, lucas, luke, richard, st1, sw1, sw2, sw3).sorted
  lazy val highestUid: Long = cluster.uids.values.max

  def runningDockerDgraphCluster: List[String] =
    Some(Process(Seq("docker", "container", "ls", "-f", "name=dgraph-unit-test-cluster-*", "-q")).lineStream.toList).filter(_.nonEmpty).getOrElse(List.empty)

  override protected def beforeAll(): Unit = {
    if (testClusterRunning && !clusterAlwaysStartUp) {
      // this file is created when dgraph-instance.insert.sh is run, see README.md, section Examples
      val source = scala.io.Source.fromFile("dgraph-instance.inserted.json")
      val json = try source.mkString finally source.close()
      cluster.uids = cluster.getUids(json)
    } else {
      if(runningDockerDgraphCluster.nonEmpty) {
        println(s"killing unit test docker dgraph cluster running from an earlier test run: ${runningDockerDgraphCluster.mkString(", ")}")
        val result = Process(Seq("docker", "container", "kill") ++ runningDockerDgraphCluster).run().exitValue()
        assert(result === 0, s"could not kill running docker container ${runningDockerDgraphCluster.mkString(", ")}")
      }

      cluster.start()
    }
    cluster.uids = cluster.uids ++ lookupGraphQlSchema()
  }

  override protected def afterAll(): Unit = {
    if (clusterAlwaysStartUp || !testClusterRunning)
      cluster.stop()
  }

  def lookupGraphQlSchema(): Map[String, Long] = {
    val query = GraphQl("""{
                          |  pred as var(func: has(<dgraph.graphql.xid>)) @filter(eq(<dgraph.graphql.xid>, "dgraph.graphql.schema"))
                          |
                          |  result (func: uid(pred)) {
                          |    uid
                          |  }
                          |}
                          |""".stripMargin)

    @tailrec
    def attempt(no: Int, limit: Int): Uid = {
      val json = DgraphExecutor(Seq(Target(cluster.grpc))).query(query)
      println(s"dgraph.graphql.schema node: $json")

      val encoder = TestEncoder()
      val nodes = encoder.getNodes(encoder.getResult(json, "result")).toSeq
      if (nodes.size === 0) {
        if (no < limit) {
          Thread.sleep(1000)
          attempt(no + 1, limit)
        } else {
          throw new IllegalStateException("Failed read graphql schema")
        }
      } else {
        val node = nodes.head
        encoder.getValue(node, "uid").asInstanceOf[Uid]
      }
    }

    val uid = attempt(1, 30)
    Map("dgraph.graphql.schema" -> uid.uid)
  }

  case class TestEncoder() extends JsonNodeInternalRowEncoder with NoColumnInfo {

    /**
     * Encodes the given Dgraph json result into InternalRows.
     *
     * @param result Json result
     * @return internal rows
     */
    override def fromJson(result: JsonArray): Iterator[InternalRow] = ???

    /**
     * Returns the schema of this table. If the table is not readable and doesn't have a schema, an
     * empty schema can be returned here.
     * From: org.apache.spark.sql.connector.catalog.Table.schema
     */
    override def schema(): StructType = ???

    /**
     * Returns the actual schema of this data source scan, which may be different from the physical
     * schema of the underlying storage, as column pruning or other optimizations may happen.
     * From: org.apache.spark.sql.connector.read.Scan.readSchema
     */
    override def readSchema(): StructType = ???

  }

}

case class DgraphCluster(name: String, version: String) {

  var process: Option[Process] = None
  var sync: Object = new Object
  var started: Boolean = false
  var uids: Map[String, Long] = Map.empty
  var portOffset: Option[Int] = None

  def grpc: String =
    portOffset.orElse(Some(0))
      .map(offset => s"localhost:${9080 + offset}")
      .get

  def grpcLocalIp: String =
    portOffset.orElse(Some(0))
      .map(offset => s"127.0.0.1:${9080 + offset}")
      .get

  def http: String =
    portOffset.orElse(Some(0))
      .map(offset => s"localhost:${8080 + offset}")
      .get

  def start(): Unit = {
    (1 to 5).iterator.flatMap { offset =>
      println(s"starting dgraph cluster (port offset=$offset)")
      portOffset = Some(offset)
      process = launchCluster(portOffset.get)
      process
    }.next()
    assert(process.isDefined)

    alterSchema()
    uids = insertData()
  }

  def stop(): Unit = {
    assert(process.isDefined)
    println("stopping dgraph cluster")
    assert(Process(Seq("docker", "container", "kill", name)).run().exitValue() == 0)
    process.foreach(_.exitValue())
  }

  val dgraphLogLines = Seq("^docker:", "^[WE].*", "^Dgraph version.*", ".*Listening on port.*", ".*CID set for cluster:*")

  def launchCluster(portOffset: Int): Option[Process] = {
    val logger = ProcessLogger(line => {
      //if (dgraphLogLines.exists(line.matches))
      println(s"Docker: $line")
      if (line.contains("CID set for cluster:")) {
        println("dgraph cluster is up")
        // notify main thread about cluster being ready
        sync.synchronized {
          started = true
          sync.notifyAll()
        }
      }
    })

    def portMap(port: Int): String = {
      val actualPort = port + portOffset
      s"$actualPort:$actualPort"
    }

    val process =
      Process(Seq(
        "docker", "run",
        "--rm",
        "--name", name,
        "-p", portMap(6080),
        "-p", portMap(8080),
        "-p", portMap(9080),
        s"dgraph/standalone:v${version}",
        "/bin/bash", "-c",
        s"dgraph zero --port_offset $portOffset &" +
          s"dgraph alpha --port_offset $portOffset --lru_mb 1024 --zero localhost:${5080 + portOffset}"
      )).run(logger)

    sync.synchronized {
      // wait for the cluster to come up (logger above observes 'CID set for cluster:')
      (1 to 3600).foreach { _ => if (!started && process.isAlive()) sync.wait(1000) }
    }

    if (started) Some(process) else None
  }

  def alterSchema(): Unit = {
    println("altering schema")
    val url = s"http://${http}/alter"
    val data =
      """  director: [uid] .
        |  name: string @index(term) .
        |  release_date: datetime @index(year) .
        |  revenue: float .
        |  running_time: int .
        |  starring: [uid] .
        |
        |  type Person {
        |    name
        |  }
        |
        |  type Film {
        |    name
        |    release_date
        |    revenue
        |    running_time
        |    starring
        |    director
        |  }
        |""".stripMargin

    attempt(url, data)
  }

  def insertData(): Map[String, Long] = {
    println("mutating dgraph")
    val url = s"http://${http}/mutate?commitNow=true"
    val headers = Seq("Content-Type" -> "application/rdf")
    val data =
      """{
        |  set {
        |   _:luke <name> "Luke Skywalker" .
        |   _:luke <dgraph.type> "Person" .
        |   _:leia <name> "Princess Leia" .
        |   _:leia <dgraph.type> "Person" .
        |   _:han <name> "Han Solo" .
        |   _:han <dgraph.type> "Person" .
        |   _:lucas <name> "George Lucas" .
        |   _:lucas <dgraph.type> "Person" .
        |   _:irvin <name> "Irvin Kernshner" .
        |   _:irvin <dgraph.type> "Person" .
        |   _:richard <name> "Richard Marquand" .
        |   _:richard <dgraph.type> "Person" .
        |
        |   _:sw1 <name> "Star Wars: Episode IV - A New Hope" .
        |   _:sw1 <release_date> "1977-05-25" .
        |   _:sw1 <revenue> "775000000" .
        |   _:sw1 <running_time> "121" .
        |   _:sw1 <starring> _:luke .
        |   _:sw1 <starring> _:leia .
        |   _:sw1 <starring> _:han .
        |   _:sw1 <director> _:lucas .
        |   _:sw1 <dgraph.type> "Film" .
        |
        |   _:sw2 <name> "Star Wars: Episode V - The Empire Strikes Back" .
        |   _:sw2 <release_date> "1980-05-21" .
        |   _:sw2 <revenue> "534000000" .
        |   _:sw2 <running_time> "124" .
        |   _:sw2 <starring> _:luke .
        |   _:sw2 <starring> _:leia .
        |   _:sw2 <starring> _:han .
        |   _:sw2 <director> _:irvin .
        |   _:sw2 <dgraph.type> "Film" .
        |
        |   _:sw3 <name> "Star Wars: Episode VI - Return of the Jedi" .
        |   _:sw3 <release_date> "1983-05-25" .
        |   _:sw3 <revenue> "572000000" .
        |   _:sw3 <running_time> "131" .
        |   _:sw3 <starring> _:luke .
        |   _:sw3 <starring> _:leia .
        |   _:sw3 <starring> _:han .
        |   _:sw3 <director> _:richard .
        |   _:sw3 <dgraph.type> "Film" .
        |
        |   _:st1 <name> "Star Trek: The Motion Picture" .
        |   _:st1 <release_date> "1979-12-07" .
        |   _:st1 <revenue> "139000000" .
        |   _:st1 <running_time> "132" .
        |   _:st1 <dgraph.type> "Film" .
        |  }
        |}""".stripMargin

    // extract the blank-node uid mapping
    getUids(attempt(url, data, headers))
  }

  @tailrec
  final def attempt(url: String, data: String, headers: Seq[(String,String)]=Seq.empty, no: Int=1, limit: Int=10, sleep: Int=500, maxSleep: Int=10000): String = {
    val response = try {
      Some(requests.post(url, headers = headers, data = RequestBlob.ByteSourceRequestBlob(data), readTimeout = 60000))
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        None
    }

    if (hasErrors(response)) {
      if (no < limit) {
        val actualSleep = math.min(sleep, maxSleep)
        Thread.sleep(actualSleep)
        attempt(url, data, headers, no + 1, limit, math.min(actualSleep * 2, maxSleep), maxSleep)
      } else {
        throw new IllegalStateException("Retry limit exceeded, giving up")
      }
    } else {
      response.get.text()
    }
  }

  def hasErrors(response: Option[Response]): Boolean = {
    if (!response.exists(_.statusCode == 200)) {
      if (response.isDefined) println(s"received status ${response.get.statusCode}: ${response.get.statusMessage}")
      true
    } else {
      val text = response.get.text()
      println(s"dgraph schema response: $text")

      val json = new Gson().fromJson(text, classOf[JsonObject])
      val errors =
        Option(json.getAsJsonArray("errors"))
          .map(_.asScala.map(_.getAsJsonObject))
          .getOrElse(Seq.empty[JsonObject])
      // dgraph schema response: {"errors":[{"message":"errIndexingInProgress. Please retry","extensions":{"code":"Error"}}]}
      errors.foreach(error => println(error.getAsJsonPrimitive("message").getAsString))
      errors.nonEmpty
    }
  }

  def getUids(json: String): Map[String, Long] = {
    // {"data":{"uids":{"han":"0x8",...}}}
    val map = new Gson()
      .fromJson(json, classOf[JsonObject])
      .getAsJsonObject("data")
      .getAsJsonObject("uids")
      .entrySet().asScala
      .map(e => e.getKey -> Uid(e.getValue.getAsString).uid)
      .toMap

    assert(map.keys.toSet == Set(
      "st1", "sw1", "sw2", "sw3",
      "lucas", "irvin", "richard",
      "leia", "luke", "han"
    ), "some expected nodes have not been inserted")

    map
  }

}

object DgraphTestCluster {

  lazy val isDgraphClusterRunning: Boolean =
    new ClusterStateProvider { }.getClusterState(Target("localhost:9080")).isDefined

  lazy val isDockerInstalled: Boolean =
    try {
      Process(Seq("docker", "--version")).run().exitValue() == 0
    } catch {
      case _: Throwable => false
    }

}
