package uk.co.gresearch.spark.dgraph

import java.sql.Timestamp

import io.dgraph.DgraphGrpc.DgraphStub
import io.dgraph.{DgraphClient, DgraphGrpc}
import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, Encoder, Encoders}

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
    override def toString: String = uid.toString
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

  case class Triple(s: Uid, p: String, o: Any)

  case class Predicate(predicateName: String, typeName: String)
  case class UidRange(first: Long, length: Long) {
    if (first < 0 || length <= 0)
      throw new IllegalArgumentException(s"UidRange first must be positive (is $first), length must be larger than zero (is $length)")
  }

  val TargetOption: String = "dgraph.target"
  val TargetsOption: String = "dgraph.targets"
  val TriplesModeOption: String = "dgraph.triples.mode"
  val TriplesModeStringOption: String = "string"
  val TriplesModeTypedOption: String = "typed"

  val PartitionerOption: String = "dgraph.partitioner"
  val SingletonPartitionerOption: String = "singleton"
  val GroupPartitionerOption: String = "group"
  val AlphaPartitionerOption: String = "alpha"
  val PredicatePartitionerOption: String = "predicate"
  val UidRangePartitionerOption: String = "uid-range"
  val PartitionerDefault: String = UidRangePartitionerOption

  val AlphaPartitionerPartitionsOption: String = "dgraph.partitioner.alpha.partitionsPerAlpha"
  val AlphaPartitionerPartitionsDefault: Int = 1
  val PredicatePartitionerPredicatesOption: String = "dgraph.partitioner.predicate.predicatesPerPartition"
  val PredicatePartitionerPredicatesDefault: Int = 1000
  val UidRangePartitionerUidsPerPartOption: String = "dgraph.partitioner.uidRange.uidsPerPartition"
  val UidRangePartitionerUidsPerPartDefault: Int = 1000

  def toChannel(target: Target): ManagedChannel = NettyChannelBuilder.forTarget(target.toString).usePlaintext().build()

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
     * @param target a target
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
     * @param target a target
     * @param targets more targets
     * @return triples DataFrame
     */
    def dgraphEdges(target: String, targets: String*): Dataset[Edge] =
      reader
        .format(EdgesSource)
        .load(Seq(target) ++ targets: _*)
        .as[Edge](edgeEncoder)

    /**
     * Loads all ndoes of a Dgraph database into a DataFrame. Requires at least one target.
     * Use dgraphNodes(targets.head, targets.tail: _*) if need to provide a Seq[String].
     * @param target a target
     * @param targets more targets
     * @return triples DataFrame
     */
    def dgraphNodes(target: String, targets: String*): Dataset[TypedNode] =
      reader
        .format(NodesSource)
        .load(Seq(target) ++ targets: _*)
        .as[TypedNode](nodeEncoder)

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
