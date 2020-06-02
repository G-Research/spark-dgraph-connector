package uk.co.gresearch.spark.dgraph

import io.dgraph.{DgraphClient, DgraphGrpc}
import io.dgraph.DgraphGrpc.DgraphStub
import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder

package object connector {
  case class Triple(s: Long, p: String, o: String)

  val TargetOption: String = "target"
  val TargetsOption: String = "targets"

  def toChannel(target: Target): ManagedChannel = NettyChannelBuilder.forTarget(target.toString).usePlaintext().build()

  def toStub(channel: ManagedChannel): DgraphStub = DgraphGrpc.newStub(channel)

  def toStub(target: Target): DgraphStub = toStub(toChannel(target))

  def getClientFromStubs(stubs: Seq[DgraphStub]): DgraphClient = new DgraphClient(stubs:_*)

  def getClientFromChannel(channels: Seq[ManagedChannel]): DgraphClient = getClientFromStubs(channels.map(toStub))

  def getClientFromTargets(targets: Seq[Target]): DgraphClient = getClientFromStubs(targets.map(toStub))

  def getClient(targets: Seq[Target]): DgraphClient = getClientFromTargets(targets)

}
