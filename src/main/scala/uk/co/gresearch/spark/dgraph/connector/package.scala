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

import java.sql.Timestamp

import io.dgraph.DgraphGrpc.DgraphStub
import io.dgraph.{DgraphClient, DgraphGrpc}
import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import org.apache.spark.sql.{DataFrame, DataFrameReader, Encoder, Encoders}

package object connector {

  val TriplesSource: String = new triples.DefaultSource().getClass.getPackage.getName
  val EdgesSource: String = new edges.DefaultSource().getClass.getPackage.getName
  val NodesSource: String = new nodes.DefaultSource().getClass.getPackage.getName

  case class StringTriple(subject: Long, predicate: String, objectString: String, objectType: String)

  case class TypedTriple(subject: Long,
                         predicate: String,
                         objectUid: Option[Long],
                         objectString: Option[String],
                         objectLong: Option[Long],
                         objectDouble: Option[Double],
                         objectTimestamp: Option[Timestamp],
                         objectBoolean: Option[Boolean],
                         objectGeo: Option[String],
                         objectPassword: Option[String],
                         objectType: String)

  case class Edge(subject: Long, predicate: String, objectUid: Long)

  case class TypedNode(subject: Long,
                       predicate: String,
                       objectString: Option[String],
                       objectLong: Option[Long],
                       objectDouble: Option[Double],
                       objectTimestamp: Option[Timestamp],
                       objectBoolean: Option[Boolean],
                       objectGeo: Option[String],
                       objectPassword: Option[String],
                       objectType: String)

  case class Uid(uid: Long) {
    if (uid < 0) throw new IllegalArgumentException(s"Uid must be positive (is $uid)")
    override def toString: String = uid.toString
    def toHexString: String = s"0x${uid.toHexString}"
    def <(other: Uid): Boolean = uid < other.uid
    def >=(other: Uid): Boolean = uid >= other.uid
    def next: Uid = Uid(uid+1)
    def before: Uid = Uid(uid-1)
  }

  object Uid {
    def apply(uid: String): Uid = Uid(toLong(uid))

    private def toLong(uid: String): Long =
      Some(uid)
        .filter(_.startsWith("0x"))
        .map(uid => java.lang.Long.valueOf(uid.substring(2), 16))
        .getOrElse(throw new IllegalArgumentException("Dgraph uid is not a long prefixed with '0x': " + uid))
  }

  case class Geo(geo: String) {
    override def toString: String = geo
  }

  case class Password(password: String) {
    override def toString: String = password
  }

  case class Predicate(predicateName: String, typeName: String)

  /**
   * Range of uids.
   * @param first first uid of range (inclusive)
   * @param until last uid of range (exclusive)
   */
  case class UidRange(first: Uid, until: Uid) {
    if (first >= until)
      throw new IllegalArgumentException(s"UidRange first uid (is $first) must be before until (is $until)")
    def length: Long = until.uid - first.uid
  }

  case class Chunk(after: Uid, length: Long) {
    if (length <= 0)
      throw new IllegalArgumentException(s"Chunk length must be larger than zero (is $length)")

    /**
     * Returns a new Chunk with the same length but given after.
     * @param after after
     * @return chunk with new after
     */
    def withAfter(after: Uid): Chunk = copy(after = after)

    /**
     * Returns a new Chunk with the same after but given length.
     * @param length length
     * @return chunk with new length
     */
    def withLength(length: Long): Chunk = copy(length = length)
  }

  // typed strings
  case class GraphQl(string: String) // technically not GraphQl but GraphQl+: https://dgraph.io/docs/query-language/
  case class Json(string: String)

  val TargetOption: String = "dgraph.target"
  val TargetsOption: String = "dgraph.targets"

  val TriplesModeOption: String = "dgraph.triples.mode"
  val TriplesModeStringOption: String = "string"
  val TriplesModeTypedOption: String = "typed"

  val NodesModeOption: String = "dgraph.nodes.mode"
  val NodesModeTypedOption: String = "typed"
  val NodesModeWideOption: String = "wide"

  val ChunkSizeOption: String = "dgraph.chunkSize"
  val ChunkSizeDefault: Int = 100000

  val PartitionerOption: String = "dgraph.partitioner"
  val SingletonPartitionerOption: String = "singleton"
  val GroupPartitionerOption: String = "group"
  val AlphaPartitionerOption: String = "alpha"
  val PredicatePartitionerOption: String = "predicate"
  val UidRangePartitionerOption: String = "uid-range"
  val PartitionerDefault: String = s"$PredicatePartitionerOption+$UidRangePartitionerOption"

  val AlphaPartitionerPartitionsOption: String = "dgraph.partitioner.alpha.partitionsPerAlpha"
  val AlphaPartitionerPartitionsDefault: Int = 1
  val PredicatePartitionerPredicatesOption: String = "dgraph.partitioner.predicate.predicatesPerPartition"
  val PredicatePartitionerPredicatesDefault: Int = 1000
  val UidRangePartitionerUidsPerPartOption: String = "dgraph.partitioner.uidRange.uidsPerPartition"
  val UidRangePartitionerUidsPerPartDefault: Int = 1000000
  val UidRangePartitionerEstimatorOption: String = "dgraph.partitioner.uidRange.estimator"
  val MaxLeaseIdEstimatorOption: String = "maxLeaseId"
  val UidRangePartitionerEstimatorDefault: String = MaxLeaseIdEstimatorOption
  // for testing purposes only
  val MaxLeaseIdEstimatorIdOption: String = s"$UidRangePartitionerEstimatorOption.$MaxLeaseIdEstimatorOption.id"

  def toChannel(target: Target): ManagedChannel = NettyChannelBuilder.forTarget(target.toString).usePlaintext().maxInboundMessageSize(24 * 1024 * 1024).build()

  def toStub(channel: ManagedChannel): DgraphStub = DgraphGrpc.newStub(channel)

  def toStub(target: Target): DgraphStub = toStub(toChannel(target))

  def getClientFromStubs(stubs: Seq[DgraphStub]): DgraphClient = new DgraphClient(stubs: _*)

  def getClientFromChannel(channels: Seq[ManagedChannel]): DgraphClient = getClientFromStubs(channels.map(toStub))

  def getClientFromTargets(targets: Seq[Target]): DgraphClient = getClientFromStubs(targets.map(toStub))

  def getClient(targets: Seq[Target]): DgraphClient = getClientFromTargets(targets)

  implicit class DgraphDataFrameReader(reader: DataFrameReader) {

    val tripleEncoder: Encoder[TypedTriple] = Encoders.product[TypedTriple]
    val edgeEncoder: Encoder[Edge] = Encoders.product[Edge]
    val nodeEncoder: Encoder[TypedNode] = Encoders.product[TypedNode]

    /**
     * Loads all triples of a Dgraph database into a DataFrame. Requires at least one target.
     * Use dgraphTriples(targets.head, targets.tail: _*) if need to provide a Seq[String].
     *
     * @param target  a target
     * @param targets more targets
     * @return triples DataFrame
     */
    def dgraphTriples(target: String, targets: String*): DataFrame =
      reader
        .format(TriplesSource)
        .load(Seq(target) ++ targets: _*)

    /**
     * Loads all edges of a Dgraph database into a DataFrame. Requires at least one target.
     * Use dgraphEdges(targets.head, targets.tail: _*) if need to provide a Seq[String].
     *
     * @param target  a target
     * @param targets more targets
     * @return edges DataFrame
     */
    def dgraphEdges(target: String, targets: String*): DataFrame =
      reader
        .format(EdgesSource)
        .load(Seq(target) ++ targets: _*)

    /**
     * Loads all ndoes of a Dgraph database into a DataFrame. Requires at least one target.
     * Use dgraphNodes(targets.head, targets.tail: _*) if need to provide a Seq[String].
     *
     * @param target  a target
     * @param targets more targets
     * @return nodes DataFrame
     */
    def dgraphNodes(target: String, targets: String*): DataFrame =
      reader
        .format(NodesSource)
        .load(Seq(target) ++ targets: _*)

  }

  implicit class RotatingSeq[T](seq: Seq[T]) {

    @scala.annotation.tailrec
    final def rotateLeft(i: Int): Seq[T] =
      if (seq.isEmpty)
        seq
      else if (i < 0)
        rotateRight(-(i % seq.length))
      else if (i >= seq.length)
        rotateLeft(i % seq.length)
      else
        seq.drop(i) ++ seq.take(i)

    @scala.annotation.tailrec
    final def rotateRight(i: Int): Seq[T] =
      if (seq.isEmpty)
        seq
      else if (i < 0)
        rotateLeft(-(i % seq.length))
      else if (i >= seq.length)
        rotateRight(i % seq.length)
      else
        seq.drop(seq.length - i) ++ seq.take(seq.length - i)

  }

}
