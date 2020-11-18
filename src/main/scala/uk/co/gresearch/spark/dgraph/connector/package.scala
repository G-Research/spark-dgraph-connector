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

package uk.co.gresearch.spark.dgraph

import java.sql.Timestamp

import io.dgraph.DgraphGrpc.DgraphStub
import io.dgraph.DgraphProto.TxnContext
import io.dgraph.{DgraphClient, DgraphGrpc}
import io.grpc.netty.NettyChannelBuilder
import io.grpc.{ManagedChannel, Status, StatusRuntimeException}
import org.apache.spark.sql.DataFrameReader

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

    def apply(uid: Any): Uid = uid match {
      case l: Long => Uid(l)
      case i: Int => Uid(i.toLong)
      case a => Uid(a.toString)
    }

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

  case class Predicate(predicateName: String, dgraphType: String, sparkType: String, isLang: Boolean = false) {
    def isProperty: Boolean = dgraphType != "uid"
    def isEdge: Boolean = dgraphType == "uid"
  }

  object Predicate {

    def apply(predicateName: String, dgraphType: String): Predicate =
      Predicate(predicateName, dgraphType, sparkDataType(dgraphType))

    def apply(predicateName: String, dgraphType: String, isLang: Boolean): Predicate =
      Predicate(predicateName, dgraphType, sparkDataType(dgraphType), isLang)

    def columnNameForPredicateName(predicateName: String): String = predicateName match {
      case "uid" => "subject"
      case x => x
    }

    def predicateNameForColumnName(columnName: String): String = columnName match {
      case "subject" => "uid"
      case x => x
    }

    def sparkDataType(dgraphDataType: String): String = dgraphDataType match {
      case "subject" => "uid"
      case "uid" => "uid"
      case "string" => "string"
      case "int" => "long"
      case "float" => "double"
      case "datetime" => "timestamp"
      case "bool" => "boolean"
      case "geo" => "geo"
      case "password" => "password"
      case "default" => "default"
      case s => throw new IllegalArgumentException(s"unknown dgraph type: $s")
    }

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
  case class GraphQl(string: String) // technically not GraphQl but GraphQl±: https://dgraph.io/docs/query-language/
  case class Json(string: String)

  val TargetOption: String = "dgraph.target"
  val TargetsOption: String = "dgraph.targets"

  val TriplesModeOption: String = "dgraph.triples.mode"
  val TriplesModeStringOption: String = "string"
  val TriplesModeTypedOption: String = "typed"

  val NodesModeOption: String = "dgraph.nodes.mode"
  val NodesModeTypedOption: String = "typed"
  val NodesModeWideOption: String = "wide"

  val TransactionModeOption: String = "dgraph.transaction.mode"
  val TransactionModeNoneOption: String = "none"
  val TransactionModeReadOption: String = "read"
  val TransactionModeDefault: String = TransactionModeNoneOption

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

  case class Transaction(context: TxnContext)

  implicit class DgraphDataFrameReader(reader: DataFrameReader) {
    /**
     * Helper to load data of a Dgraph database into a DataFrame.
     */
    def dgraph: DgraphReader = DgraphReader(reader)
  }

  implicit class AnyValue(value: Any) {
    def toLong: Long = value match {
      case v: Int => v.toLong
      case v: Long => v
      case v: String => v.toLong
      case _ => value.asInstanceOf[Long]
    }
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

  implicit class ExtendedThrowable(throwable: Throwable) {
    def causedByResourceExhausted(): Boolean = isCausedByResourceExhausted(throwable)
  }

  @scala.annotation.tailrec
  private def isCausedByResourceExhausted(throwable: Throwable): Boolean = throwable match {
    case sre: StatusRuntimeException =>
      sre.getStatus.getCode == Status.Code.RESOURCE_EXHAUSTED
    case _ =>
      throwable.getCause != null && isCausedByResourceExhausted(throwable.getCause)
  }

}
