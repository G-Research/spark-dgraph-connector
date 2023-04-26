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

package uk.co.gresearch.spark.dgraph.connector.sources

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, In, Literal}
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark._
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.{DgraphCluster, DgraphTestCluster}

import java.sql.Timestamp
import scala.reflect.runtime.universe.{TypeTag, typeTag}

class TestNodeSource extends AnyFunSpec
  with ConnectorSparkTestSession with DgraphTestCluster
  with FilterPushdownTestHelper
  with ProjectionPushDownTestHelper {

  import spark.implicits._

  def removeDgraphTypedNodes(nodes: Dataset[TypedNode]): Dataset[TypedNode] = {
    val dgraphNodeUids =
      nodes
        .where($"predicate" === "dgraph.type" && $"objectString".startsWith("dgraph."))
        .select($"subject").distinct().as[Long].collect()
    nodes.where(!$"subject".isin(dgraphNodeUids: _*))
  }

  def removeDgraphWideNodes[T](nodes: Dataset[T]): Dataset[T] = {
    val dgraphNodeUids =
      nodes
        .where($"`dgraph.type`".startsWith("dgraph."))
        .select($"subject").distinct().as[Long].collect()
    nodes.where(!$"subject".isin(dgraphNodeUids: _*))
  }

  describe("NodeDataSource") {

    lazy val expecteds = NodesSourceExpecteds(dgraph)
    lazy val expectedTypedNodes = expecteds.getExpectedTypedNodes
    lazy val expectedWideNodes = expecteds.getExpectedWideNodes
    lazy val expectedWideSchema: StructType = expecteds.getExpectedWideNodeSchema

    def doTestLoadTypedNodes(load: () => DataFrame, expected: Set[TypedNode] = expectedTypedNodes, removeDgraphNodes: Boolean = true): Unit = {
      val nodes = (if (removeDgraphNodes) removeDgraphTypedNodes(load().as[TypedNode]) else load().as[TypedNode]).collect().toSet
      assert(nodes.toSeq.sortBy(n => (n.subject, n.predicate)).mkString("\n") === expected.toSeq.sortBy(n => (n.subject, n.predicate)).mkString("\n"))
    }

    def doTestLoadWideNodes(load: () => DataFrame,
                            expectedNodes: Set[Row] = expectedWideNodes,
                            expectedSchema: StructType = expectedWideSchema,
                            removeDgraphNodes: Boolean = true): Unit = {
      val df = if (removeDgraphNodes) removeDgraphWideNodes(load()) else load()
      val nodes = df.collect().toSet
      assert(df.schema === expectedSchema)
      assert(nodes === expectedNodes)
    }

    it("should load nodes via path") {
      doTestLoadTypedNodes(() =>
        reader
          .format(NodesSource)
          .load(dgraph.target)
      )
    }

    it("should load nodes via paths") {
      doTestLoadTypedNodes(() =>
        reader
          .format(NodesSource)
          .load(dgraph.target, dgraph.targetLocalIp)
      )
    }

    it("should load nodes via target option") {
      doTestLoadTypedNodes(() =>
        reader
          .format(NodesSource)
          .option(TargetOption, dgraph.target)
          .load()
      )
    }

    it("should load nodes via targets option") {
      doTestLoadTypedNodes(() =>
        reader
          .format(NodesSource)
          .option(TargetsOption, s"""["${dgraph.target}","${dgraph.targetLocalIp}"]""")
          .load()
      )
    }

    it("should load nodes via implicit dgraph target") {
      doTestLoadTypedNodes(() =>
        reader
          .dgraph.nodes(dgraph.target)
      )
    }

    it("should load nodes via implicit dgraph targets") {
      doTestLoadTypedNodes(() =>
        reader
          .dgraph.nodes(dgraph.target)
      )
    }

    it("should load typed nodes") {
      doTestLoadTypedNodes(() =>
        reader
          .option(NodesModeOption, NodesModeTypedOption)
          .dgraph.nodes(dgraph.target)
      )
    }

    it("should load wide nodes") {
      doTestLoadWideNodes(() =>
        reader
          .option(NodesModeOption, NodesModeWideOption)
          .dgraph.nodes(dgraph.target)
      )
    }

    lazy val availableTypedNodes =
      spark
        .read
        .option(NodesModeOption, NodesModeTypedOption)
        .dgraph.nodes(dgraph.target)
        .as[TypedNode]
        .collect()
        .toSet

    // These reserved predicate tests assume some reserved predicates exist in the test Dgraph data
    // See TestDgraphTestCluster for those assumptions
    it("should load typed-object nodes and include reserved predicates") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeTypedOption)
          .option(IncludeReservedPredicatesOption, "dgraph.type")
          .dgraph.nodes(dgraph.target),
        availableTypedNodes.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate == "dgraph.type"),
        removeDgraphNodes = false
      )

      doTestLoadTypedNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeTypedOption)
          .option(IncludeReservedPredicatesOption, "dgraph.type,dgraph.graphql.xid")
          .dgraph.nodes(dgraph.target),
        availableTypedNodes.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate == "dgraph.type" || triple.predicate == "dgraph.graphql.xid"),
        removeDgraphNodes = false
      )

      doTestLoadTypedNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeTypedOption)
          .option(IncludeReservedPredicatesOption, "dgraph.graphql.*")
          .dgraph.nodes(dgraph.target),
        availableTypedNodes.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate.startsWith("dgraph.graphql.")),
        removeDgraphNodes = false
      )

      doTestLoadTypedNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeTypedOption)
          .option(IncludeReservedPredicatesOption, "dgraph.type,dgraph.graphql.*")
          .dgraph.nodes(dgraph.target),
        availableTypedNodes.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate == "dgraph.type" || triple.predicate.startsWith("dgraph.graphql.")),
        removeDgraphNodes = false
      )
    }

    it("should load typed-object nodes and exclude reserved predicates") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeTypedOption)
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.xid")
          .dgraph.nodes(dgraph.target),
        availableTypedNodes.filter(triple => triple.predicate != "dgraph.graphql.xid"),
        removeDgraphNodes = false
      )

      doTestLoadTypedNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeTypedOption)
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.schema,dgraph.graphql.xid")
          .dgraph.nodes(dgraph.target),
        availableTypedNodes.filter(triple => !Set("dgraph.graphql.schema", "dgraph.graphql.xid").contains(triple.predicate)),
        removeDgraphNodes = false
      )

      doTestLoadTypedNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeTypedOption)
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.*")
          .dgraph.nodes(dgraph.target),
        availableTypedNodes.filter(triple => !triple.predicate.startsWith("dgraph.graphql.")),
        removeDgraphNodes = false
      )

      doTestLoadTypedNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeTypedOption)
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.*,dgraph.type")
          .dgraph.nodes(dgraph.target),
        availableTypedNodes.filter(triple => !triple.predicate.startsWith("dgraph.graphql.") && triple.predicate != "dgraph.type"),
        removeDgraphNodes = false
      )
    }

    it("should load typed-object nodes and include/exclude reserved predicates") {
      doTestLoadTypedNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeTypedOption)
          .option(IncludeReservedPredicatesOption, "dgraph.graphql.*")
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.xid")
          .dgraph.nodes(dgraph.target),
        availableTypedNodes.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate.startsWith("dgraph.graphql.") && triple.predicate != "dgraph.graphql.xid"),
        removeDgraphNodes = false
      )

      doTestLoadTypedNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeTypedOption)
          .option(IncludeReservedPredicatesOption, "dgraph.graphql.*")
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.x*")
          .dgraph.nodes(dgraph.target),
        availableTypedNodes.filter(triple => !triple.predicate.startsWith("dgraph.") || triple.predicate.startsWith("dgraph.graphql.") && !triple.predicate.startsWith("dgraph.graphql.x")),
        removeDgraphNodes = false
      )
    }

    lazy val (availableWideNodes, availableWideSchema) = {
      val wideNodes =
        spark
          .read
          .option(NodesModeOption, NodesModeWideOption)
          .dgraph.nodes(dgraph.target)
      println("available wide nodes:")
      wideNodes.show()
      (wideNodes.collect().toSet, wideNodes.schema)
    }

    def expectedWideNodesFiltered(filter: StructField => Boolean): Set[Row] = {
      val filteredIndices = availableWideSchema.zipWithIndex.filter(x => filter(x._1)).map(_._2)
      availableWideNodes.map(select(filteredIndices: _*))
        // drop all-null (except the first column) rows
        .filter(_.toSeq.tail.map(Option(_)).exists(_.isDefined))
    }

    def expectedWideSchemaFiltered(filter: StructField => Boolean): StructType =
      availableWideSchema.copy(fields = availableWideSchema.fields.filter(filter))

    it("should load wide nodes and include reserved predicates") {
      doTestLoadWideNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeWideOption)
          .option(IncludeReservedPredicatesOption, "dgraph.type")
          .dgraph.nodes(dgraph.target),
        expectedWideNodesFiltered(predicate => !predicate.name.startsWith("dgraph.") || predicate.name == "dgraph.type"),
        expectedWideSchemaFiltered(predicate => !predicate.name.startsWith("dgraph.") || predicate.name == "dgraph.type"),
        removeDgraphNodes = false
      )

      doTestLoadWideNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeWideOption)
          .option(IncludeReservedPredicatesOption, "dgraph.type,dgraph.graphql.xid")
          .dgraph.nodes(dgraph.target),
        expectedWideNodesFiltered(predicate => !predicate.name.startsWith("dgraph.") || predicate.name == "dgraph.type" || predicate.name == "dgraph.graphql.xid"),
        expectedWideSchemaFiltered(predicate => !predicate.name.startsWith("dgraph.") || predicate.name == "dgraph.type" || predicate.name == "dgraph.graphql.xid"),
        removeDgraphNodes = false
      )

      doTestLoadWideNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeWideOption)
          .option(IncludeReservedPredicatesOption, "dgraph.graphql.*")
          .dgraph.nodes(dgraph.target),
        expectedWideNodesFiltered(predicate => !predicate.name.startsWith("dgraph.") || predicate.name.startsWith("dgraph.graphql.")),
        expectedWideSchemaFiltered(predicate => !predicate.name.startsWith("dgraph.") || predicate.name.startsWith("dgraph.graphql.")),
        removeDgraphNodes = false
      )

      doTestLoadWideNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeWideOption)
          .option(IncludeReservedPredicatesOption, "dgraph.type,dgraph.graphql.*")
          .dgraph.nodes(dgraph.target),
        expectedWideNodesFiltered(predicate => !predicate.name.startsWith("dgraph.") || predicate.name == "dgraph.type" || predicate.name.startsWith("dgraph.graphql.")),
        expectedWideSchemaFiltered(predicate => !predicate.name.startsWith("dgraph.") || predicate.name == "dgraph.type" || predicate.name.startsWith("dgraph.graphql.")),
        removeDgraphNodes = false
      )
    }

    it("should load wide nodes and exclude reserved names") {
      doTestLoadWideNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeWideOption)
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.xid")
          .dgraph.nodes(dgraph.target),
        expectedWideNodesFiltered(predicate => predicate.name != "dgraph.graphql.xid"),
        expectedWideSchemaFiltered(predicate => predicate.name != "dgraph.graphql.xid"),
        removeDgraphNodes = false
      )

      doTestLoadWideNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeWideOption)
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.schema,dgraph.graphql.xid")
          .dgraph.nodes(dgraph.target),
        expectedWideNodesFiltered(predicate => !Set("dgraph.graphql.schema", "dgraph.graphql.xid").contains(predicate.name)),
        expectedWideSchemaFiltered(predicate => !Set("dgraph.graphql.schema", "dgraph.graphql.xid").contains(predicate.name)),
        removeDgraphNodes = false
      )

      doTestLoadWideNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeWideOption)
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.*")
          .dgraph.nodes(dgraph.target),
        expectedWideNodesFiltered(predicate => !predicate.name.startsWith("dgraph.graphql.")),
        expectedWideSchemaFiltered(predicate => !predicate.name.startsWith("dgraph.graphql.")),
        removeDgraphNodes = false
      )

      doTestLoadWideNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeWideOption)
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.*,dgraph.type")
          .dgraph.nodes(dgraph.target),
        expectedWideNodesFiltered(predicate => !predicate.name.startsWith("dgraph.graphql.") && predicate.name != "dgraph.type"),
        expectedWideSchemaFiltered(predicate => !predicate.name.startsWith("dgraph.graphql.") && predicate.name != "dgraph.type"),
        removeDgraphNodes = false
      )
    }

    it("should load wide nodes and include/exclude reserved names") {
      doTestLoadWideNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeWideOption)
          .option(IncludeReservedPredicatesOption, "dgraph.graphql.*")
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.xid")
          .dgraph.nodes(dgraph.target),
        expectedWideNodesFiltered(predicate => !predicate.name.startsWith("dgraph.") || predicate.name.startsWith("dgraph.graphql.") && predicate.name != "dgraph.graphql.xid"),
        expectedWideSchemaFiltered(predicate => !predicate.name.startsWith("dgraph.") || predicate.name.startsWith("dgraph.graphql.") && predicate.name != "dgraph.graphql.xid"),
        removeDgraphNodes = false
      )

      doTestLoadWideNodes(() =>
        spark
          .read
          .option(NodesModeOption, NodesModeWideOption)
          .option(IncludeReservedPredicatesOption, "dgraph.graphql.*")
          .option(ExcludeReservedPredicatesOption, "dgraph.graphql.x*")
          .dgraph.nodes(dgraph.target),
        expectedWideNodesFiltered(predicate => !predicate.name.startsWith("dgraph.") || predicate.name.startsWith("dgraph.graphql.") && !predicate.name.startsWith("dgraph.graphql.x")),
        expectedWideSchemaFiltered(predicate => !predicate.name.startsWith("dgraph.") || predicate.name.startsWith("dgraph.graphql.") && !predicate.name.startsWith("dgraph.graphql.x")),
        removeDgraphNodes = false
      )
    }

    it("should load wide nodes with predicate partitioner") {
      doTestLoadWideNodes(() =>
        reader
          .options(Map(
            NodesModeOption -> NodesModeWideOption,
            PartitionerOption -> PredicatePartitionerOption,
            PredicatePartitionerPredicatesOption -> "1"
          ))
          .dgraph.nodes(dgraph.target)
      )
    }

    it("should load wide nodes with predicate partitioner and uid ranges") {
      doTestLoadWideNodes(() =>
        reader
          .options(Map(
            NodesModeOption -> NodesModeWideOption,
            PartitionerOption -> s"$PredicatePartitionerOption+$UidRangePartitionerOption",
            PredicatePartitionerPredicatesOption -> "1",
            UidRangePartitionerUidsPerPartOption -> "1",
            MaxLeaseIdEstimatorIdOption -> dgraph.highestUid.toString
          ))
          .dgraph.nodes(dgraph.target)
      )
    }

    it("should load typed nodes in chunks") {
      // it is hard to test data are really read in chunks, but we can test the data are correct
      doTestLoadTypedNodes(() =>
        reader
          .options(Map(
            NodesModeOption -> NodesModeTypedOption,
            PartitionerOption -> PredicatePartitionerOption,
            PredicatePartitionerPredicatesOption -> "2",
            ChunkSizeOption -> "3"
          ))
          .dgraph.nodes(dgraph.target)
      )
    }

    it("should encode TypedNode") {
      val rows =
        removeDgraphTypedNodes(
        reader
          .format(NodesSource)
          .load(dgraph.target)
          .as[TypedNode]
        ).collectAsList()
      assert(rows.size() === 49)
    }

    it("should fail without target") {
      assertThrows[IllegalArgumentException] {
        reader
          .format(NodesSource)
          .load()
      }
    }

    it("should fail with unknown mode") {
      assertThrows[IllegalArgumentException] {
        reader
          .format(NodesSource)
          .option(NodesModeOption, "unknown")
          .load()
      }
    }

    it("should load as a single partition") {
      val target = dgraph.target
      val targets = Seq(Target(target))
      val partitions =
        reader
          .option(PartitionerOption, SingletonPartitionerOption)
          .dgraph.nodes(target)
          .rdd
          .partitions.flatMap {
            case p: DataSourceRDDPartition => p.inputPartitions
            case _ => Seq.empty
          }

      val predicates = Set(
        Predicate("dgraph.type", "string"),
        Predicate("name", "string"),
        Predicate("release_date", "datetime"),
        Predicate("revenue", "float"),
        Predicate("running_time", "int"),
        Predicate("title", "string"),
      )
      assert(partitions === Seq(Partition(targets).has(predicates).langs(Set("title"))))
    }

    it("should load as a predicate partitions") {
      val target = dgraph.target
      val partitions =
        reader
          .option(PartitionerOption, PredicatePartitionerOption)
          .option(PredicatePartitionerPredicatesOption, "2")
          .dgraph.nodes(target)
          .rdd
          .partitions.flatMap {
            case p: DataSourceRDDPartition => p.inputPartitions
            case _ => Seq.empty
          }

      val expected = Seq(
        Partition(Seq(Target(dgraph.target))).has(Set("release_date", "running_time"), Set.empty).getAll,
        Partition(Seq(Target(dgraph.target))).has(Set("dgraph.type", "name"), Set.empty).getAll,
        Partition(Seq(Target(dgraph.target))).has(Set("revenue", "title"), Set.empty).langs(Set("title")).getAll
      )

      assert(partitions === expected)
    }

    it("should partition data") {
      val target = dgraph.target
      val partitions = {
        removeDgraphTypedNodes(
          reader
            .options(Map(
              PartitionerOption -> UidRangePartitionerOption,
              UidRangePartitionerUidsPerPartOption -> "7",
              MaxLeaseIdEstimatorIdOption -> dgraph.highestUid.toString
            ))
            .dgraph.nodes(target)
            .as[TypedNode]
        )
          .toDF()
          .mapPartitions(part => Iterator(part.map(_.getLong(0)).toSet))
          .collect()
      }

      // we retrieve partitions in chunks of 7 uids, if there are uids allocated but unused then we get partitions with less than 7 uids
      val allUidInts = dgraph.allUids.map(_.toInt).toSet
      val expected = (1 to dgraph.highestUid.toInt).grouped(7).map(_.toSet intersect allUidInts).toSeq
      assert(partitions === expected)
    }

    lazy val typedNodes =
      removeDgraphTypedNodes(
        reader
          .options(Map(
            NodesModeOption -> NodesModeTypedOption,
            PartitionerOption -> PredicatePartitionerOption,
            PredicatePartitionerPredicatesOption -> "2"
          ))
          .dgraph.nodes(dgraph.target)
          .as[TypedNode]
      )
    lazy val typedNodesSinglePredicatePerPartition =
      removeDgraphTypedNodes(
        reader
          .options(Map(
            NodesModeOption -> NodesModeTypedOption,
            PartitionerOption -> PredicatePartitionerOption,
            PredicatePartitionerPredicatesOption -> "1"
          ))
          .dgraph.nodes(dgraph.target)
          .as[TypedNode]
      )

    lazy val wideNodes =
      removeDgraphWideNodes(
        reader
          .options(Map(
            NodesModeOption -> NodesModeWideOption,
            PartitionerOption -> PredicatePartitionerOption
          ))
          .dgraph.nodes(dgraph.target)
      )

    def doTestFilterPushDown[T](df: Dataset[T], condition: Column, expectedFilters: Set[Filter], expectedUnpushed: Seq[Expression] = Seq.empty, expectedDs: Set[T]): Unit = {
      doTestFilterPushDownDf(df, condition, expectedFilters, expectedUnpushed, expectedDs)
    }

    def doTestsFilterPushDown(condition: Column,
                              expectedFilters: Set[Filter],
                              expectedUnpushed: Seq[Expression] = Seq.empty,
                              expectedTypedDsFilter: TypedNode => Boolean,
                              expectedWideDsFilter: Row => Boolean): Unit = {
      doTestFilterPushDownDf(typedNodes, condition, expectedFilters, expectedUnpushed, expectedTypedNodes.filter(expectedTypedDsFilter))
      doTestFilterPushDownDf(wideNodes, condition, expectedFilters, expectedUnpushed, expectedWideNodes.filter(expectedWideDsFilter))
    }

    it("should push subject filters") {
      doTestsFilterPushDown(
        $"subject" === dgraph.leia,
        Set(SubjectIsIn(Uid(dgraph.leia))),
        Seq.empty,
        (n: TypedNode) => n.subject == dgraph.leia,
        (r: Row) => r.getLong(0) == dgraph.leia
      )

      doTestsFilterPushDown(
        $"subject".isin(dgraph.leia),
        Set(SubjectIsIn(Uid(dgraph.leia))),
        Seq.empty,
        (n: TypedNode) => n.subject == dgraph.leia,
        (r: Row) => r.getLong(0) == dgraph.leia
      )

      doTestsFilterPushDown(
        $"subject".isin(dgraph.leia, dgraph.luke),
        Set(SubjectIsIn(Uid(dgraph.leia), Uid(dgraph.luke))),
        Seq.empty,
        (n: TypedNode) => Set(dgraph.leia, dgraph.luke).contains(n.subject),
        (r: Row) => Set(dgraph.leia, dgraph.luke).contains(r.getLong(0))
      )
    }

    describe("typed nodes") {

      it("should push predicate filters") {
        doTestFilterPushDown(typedNodes,
          $"predicate" === "name",
          Set(IntersectPredicateNameIsIn("name")),
          expectedDs = expectedTypedNodes.filter(_.predicate == "name")
        )

        doTestFilterPushDown(typedNodes,
          $"predicate".isin("name"),
          Set(IntersectPredicateNameIsIn("name")),
          expectedDs = expectedTypedNodes.filter(_.predicate == "name")
        )

        doTestFilterPushDown(typedNodes,
          $"predicate".isin("name", "starring"),
          Set(IntersectPredicateNameIsIn("name", "starring")),
          expectedDs = expectedTypedNodes.filter(t => Set("name", "starring").contains(t.predicate))
        )
      }

      it("should push object type filters") {
        doTestFilterPushDown(typedNodes,
          $"objectType" === "string",
          Set(ObjectTypeIsIn("string")),
          expectedDs = expectedTypedNodes.filter(_.objectType == "string")
        )

        doTestFilterPushDown(typedNodes,
          $"objectType".isin("string"),
          Set(ObjectTypeIsIn("string")),
          expectedDs = expectedTypedNodes.filter(_.objectType == "string")
        )

        doTestFilterPushDown(typedNodes,
          $"objectType".isin("string", "long"),
          Set(ObjectTypeIsIn("string", "long")),
          expectedDs = expectedTypedNodes.filter(t => Set("string", "long").contains(t.objectType))
        )
      }

      it("should push object value filters") {
        doTestFilterPushDown(typedNodes,
          $"objectString".isNotNull,
          Set(ObjectTypeIsIn("string")),
          expectedDs = expectedTypedNodes.filter(_.objectString.isDefined)
        )
        doTestFilterPushDown(typedNodes,
          $"objectString".isNotNull && $"objectLong".isNotNull,
          Set(AlwaysFalse),
          expectedDs = Set.empty
        )

        doTestFilterPushDown(typedNodes,
          $"objectString" === "Person",
          Set(ObjectValueIsIn("Person"), ObjectTypeIsIn("string")),
          // With multiple predicates per partition, we cannot filter for object values
          Seq(EqualTo(AttributeReference("objectString", StringType, nullable = true)(), Literal("Person"))),
          expectedDs = expectedTypedNodes.filter(_.objectString.exists(_.equals("Person")))
        )
        doTestFilterPushDown(typedNodesSinglePredicatePerPartition,
          $"objectString" === "Person",
          Set(ObjectValueIsIn("Person"), ObjectTypeIsIn("string")),
          expectedDs = expectedTypedNodes.filter(_.objectString.exists(_.equals("Person")))
        )

        doTestFilterPushDown(typedNodes,
          $"objectString".isin("Person"),
          Set(ObjectValueIsIn("Person"), ObjectTypeIsIn("string")),
          // With multiple predicates per partition, we cannot filter for object values
          Seq(EqualTo(AttributeReference("objectString", StringType, nullable = true)(), Literal("Person"))),
          expectedDs = expectedTypedNodes.filter(_.objectString.exists(_.equals("Person")))
        )
        doTestFilterPushDown(typedNodesSinglePredicatePerPartition,
          $"objectString".isin("Person"),
          Set(ObjectValueIsIn("Person"), ObjectTypeIsIn("string")),
          expectedDs = expectedTypedNodes.filter(_.objectString.exists(_.equals("Person")))
        )

        doTestFilterPushDown(typedNodes,
          $"objectString".isin("Person", "Film"),
          Set(ObjectValueIsIn("Person", "Film"), ObjectTypeIsIn("string")),
          // With multiple predicates per partition, we cannot filter for object values
          Seq(In(AttributeReference("objectString", StringType, nullable = true)(), Seq(Literal("Person"), Literal("Film")))),
          expectedDs = expectedTypedNodes.filter(_.objectString.exists(s => Set("Person", "Film").contains(s)))
        )
        doTestFilterPushDown(typedNodesSinglePredicatePerPartition,
          $"objectString".isin("Person", "Film"),
          Set(ObjectValueIsIn("Person", "Film"), ObjectTypeIsIn("string")),
          expectedDs = expectedTypedNodes.filter(_.objectString.exists(s => Set("Person", "Film").contains(s)))
        )

        doTestFilterPushDown(typedNodes,
          $"objectString" === "Person" && $"objectLong" === 1,
          Set(AlwaysFalse),
          // With multiple predicates per partition, we cannot filter for object values
          Seq(
            EqualTo(AttributeReference("objectString", StringType, nullable = true)(), Literal("Person")),
            EqualTo(AttributeReference("objectLong", LongType, nullable = true)(), Literal(1L))
          ),
          expectedDs = Set.empty
        )
        doTestFilterPushDown(typedNodesSinglePredicatePerPartition,
          $"objectString" === "Person" && $"objectLong" === 1,
          Set(AlwaysFalse),
          expectedDs = Set.empty
        )

      }

      it("should not push projection") {
        doTestProjectionPushDownDf(
          typedNodes.toDF,
          Seq($"subject", $"predicate", $"objectString"),
          None,
          expectedTypedNodes.toSeq.toDF.collect().map(select(0, 1, 2)).toSet
        )
      }

    }

    describe("wide node") {

      it("should push predicate value filters") {
        val columns = wideNodes.columns

        doTestFilterPushDown(wideNodes,
          $"name".isNotNull,
          Set(PredicateNameIs("name")),
          expectedDs = expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("name"))).isDefined)
        )

        doTestFilterPushDown(wideNodes,
          $"name".isNotNull && $"running_time".isNotNull,
          Set(PredicateNameIs("name"), PredicateNameIs("running_time")),
          expectedDs = expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("name"))).isDefined && !r.isNullAt(columns.indexOf("running_time")))
        )

        doTestFilterPushDown(wideNodes,
          $"title" === "Star Wars: Episode IV - A New Hope",
          Set(PredicateNameIs("title"), SinglePredicateValueIsIn("title", Set("Star Wars: Episode IV - A New Hope"))),
          // With multiple predicates per partition, we cannot filter for object values
          Seq(
            EqualTo(AttributeReference("title", StringType, nullable = true)(), Literal("Star Wars: Episode IV - A New Hope")),
          ),
          expectedDs = expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("title"))).exists(_.equals("Star Wars: Episode IV - A New Hope")))
        )

        doTestFilterPushDown(wideNodes,
          $"title".isin("Star Wars: Episode IV - A New Hope"),
          Set(PredicateNameIs("title"), SinglePredicateValueIsIn("title", Set("Star Wars: Episode IV - A New Hope"))),
          // With multiple predicates per partition, we cannot filter for object values
          Seq(
            EqualTo(AttributeReference("title", StringType, nullable = true)(), Literal("Star Wars: Episode IV - A New Hope")),
          ),
          expectedDs = expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("title"))).exists(_.equals("Star Wars: Episode IV - A New Hope")))
        )

        doTestFilterPushDown(wideNodes,
          $"title".isin("Star Wars: Episode IV - A New Hope", "Star Wars: Episode V - The Empire Strikes Back"),
          Set(SinglePredicateValueIsIn("title", Set("Star Wars: Episode IV - A New Hope", "Star Wars: Episode V - The Empire Strikes Back"))),
          // With multiple predicates per partition, we cannot filter for object values
          Seq(
            In(AttributeReference("title", StringType, nullable = true)(), Seq(
              Literal("Star Wars: Episode IV - A New Hope"),
              Literal("Star Wars: Episode V - The Empire Strikes Back")
            )),
          ),
          expectedDs = expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("title"))).exists(
            Set("Star Wars: Episode IV - A New Hope", "Star Wars: Episode V - The Empire Strikes Back").contains
          ))
        )

        doTestFilterPushDown(wideNodes,
          $"title" === "Star Wars: Episode IV - A New Hope" && $"running_time" === 121,
          Set(PredicateNameIs("title"), PredicateNameIs("running_time"), SinglePredicateValueIsIn("title", Set("Star Wars: Episode IV - A New Hope")), SinglePredicateValueIsIn("running_time", Set(121))),
          // With multiple predicates per partition, we cannot filter for object values
          Seq(
            EqualTo(AttributeReference("title", StringType, nullable = true)(), Literal("Star Wars: Episode IV - A New Hope")),
            EqualTo(AttributeReference("running_time", LongType, nullable = true)(), Literal(121L)),
          ),
          expectedDs = expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("title"))).exists(_.equals("Star Wars: Episode IV - A New Hope")) && Option(r.getLong(columns.indexOf("running_time"))).exists(_.equals(121L)))
        )

        val expected = expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("name"))).exists(_.equals("Luke Skywalker")) && (if (r.isNullAt(columns.indexOf("running_time"))) None else Some(r.getLong(columns.indexOf("running_time")))).exists(_.equals(121)))
        assert(expected.isEmpty, "expect empty result for this query, check query")
        doTestFilterPushDown(wideNodes,
          $"name" === "Luke Skywalker" && $"running_time" === 121,
          Set(PredicateNameIs("name"), PredicateNameIs("running_time"), SinglePredicateValueIsIn("name", Set("Luke Skywalker")), SinglePredicateValueIsIn("running_time", Set(121))),
          // With multiple predicates per partition, we cannot filter for object values
          Seq(
            EqualTo(AttributeReference("name", StringType, nullable = true)(), Literal("Luke Skywalker")),
            EqualTo(AttributeReference("running_time", LongType, nullable = true)(), Literal(121L)),
          ),
          expectedDs = expected
        )
      }

      val expectedPredicates: Seq[Predicate] = Seq(
        Predicate("uid", "subject"),
        Predicate("dgraph.graphql.schema", "string"),
        Predicate("dgraph.graphql.xid", "string"),
        Predicate("dgraph.type", "string"),
        Predicate("name", "string"),
        Predicate("title", "string", isLang = true),
        Predicate("release_date", "datetime"),
        Predicate("revenue", "float"),
        Predicate("running_time", "int")
      )

      it("should push projection") {
        doTestProjectionPushDownDf(
          wideNodes,
          Seq($"subject", $"`dgraph.type`", $"name"),
          Some(expectedPredicates.filter(p => Set("uid", "dgraph.type", "name").contains(p.predicateName))),
          expectedWideNodes.map(select(0, 1, 2))
        )
      }

      it("should push projection reordered") {
        doTestProjectionPushDownDf(
          wideNodes,
          Seq($"subject", $"name", $"`dgraph.type`"),
          Some(expectedPredicates.filter(p => Set("uid", "name", "dgraph.type").contains(p.predicateName))),
          expectedWideNodes.map(select(0, 2, 1))
        )
      }

      it("should push projection without subject") {
        doTestProjectionPushDownDf(
          wideNodes,
          Seq($"`dgraph.type`", $"name", $"revenue"),
          Some(expectedPredicates.filter(p => Set("dgraph.type", "name", "revenue").contains(p.predicateName))),
          expectedWideNodes.map(select(1, 2, 4))
        )
      }

      it("should push projection with subject only") {
        doTestProjectionPushDownDf(
          wideNodes,
          Seq($"subject"),
          Some(expectedPredicates.filter(p => Set("uid").contains(p.predicateName))),
          expectedWideNodes.map(select(0))
        )
      }

      it("should not push no projection") {
        doTestProjectionPushDownDf(
          wideNodes,
          Seq.empty,
          None,
          expectedWideNodes
        )
      }

      it("should not push full projection") {
        doTestProjectionPushDownDf(
          wideNodes,
          wideNodes.columns.map(c => col(s"`$c`")),
          None,
          expectedWideNodes
        )
      }

      it("should push full projection reordered") {
        doTestProjectionPushDownDf(
          wideNodes,
          wideNodes.columns.reverse.map(c => col(s"`$c`")),
          None,
          expectedWideNodes.map(select(6, 5, 4, 3, 2, 1, 0))
        )
      }

      it("should push projection with isNotNull") {
        doTestProjectionPushDownDf(
          wideNodes.where($"subject".isNotNull && $"`dgraph.type`".isNotNull && $"name".isNotNull),
          Seq($"subject", $"`dgraph.type`", $"name"),
          Some(expectedPredicates.filter(p => Set("uid", "dgraph.type", "name").contains(p.predicateName))),
          expectedWideNodes.filter(row => !row.isNullAt(0) && !row.isNullAt(1) && !row.isNullAt(2)).map(select(0, 1, 2))
        )
      }

    }

    it("should push filters for backticked columns") {
      val columns = wideNodes.columns

      doTestFilterPushDown(wideNodes,
        $"`dgraph.type`".isNotNull,
        Set(PredicateNameIs("dgraph.type")),
        expectedDs = expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("dgraph.type"))).isDefined)
      )

      doTestFilterPushDown(wideNodes,
        $"`dgraph.type`" === "Film",
        Set(PredicateNameIs("dgraph.type"), SinglePredicateValueIsIn("dgraph.type", Set("Film"))),
        // With multiple predicates per partition, we cannot filter for object values
        Seq(EqualTo(AttributeReference("dgraph.type", StringType, nullable = true)(), Literal("Film"))),
        expectedDs = expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("dgraph.type"))).exists(_.equals("Film")))
      )

      doTestFilterPushDown(wideNodes,
        $"`dgraph.type`".isin("Film"),
        Set(PredicateNameIs("dgraph.type"), SinglePredicateValueIsIn("dgraph.type", Set("Film"))),
        // With multiple predicates per partition, we cannot filter for object values
        Seq(EqualTo(AttributeReference("dgraph.type", StringType, nullable = true)(), Literal("Film"))),
        expectedDs = expectedWideNodes.filter(r => Option(r.getString(columns.indexOf("dgraph.type"))).exists(_.equals("Film")))
      )

    }

    it("should provide expected wide nodes") {
      expecteds.getExpectedTypedNodeDf(spark).show(false)
    }

  }

}

case class NodesSourceExpecteds(cluster: DgraphCluster) {

  def getDataFrame[T <: Product : TypeTag](rows: Set[T], spark: SparkSession): DataFrame =
    spark.createDataset(rows.toSeq)(Encoders.product[T]).toDF()

  def getExpectedTypedNodeDf(spark: SparkSession): DataFrame =
    getDataFrame(getExpectedTypedNodes, spark)(typeTag[TypedNode])

  def getExpectedTypedNodes: Set[TypedNode] =
    Set(
      TypedNode(cluster.st1, "dgraph.type", Some("Film"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.st1, "title@en", Some("Star Trek: The Motion Picture"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.st1, "release_date", None, None, None, Some(Timestamp.valueOf("1979-12-07 00:00:00.0")), None, None, None, "timestamp"),
      TypedNode(cluster.st1, "revenue", None, None, Some(1.39E8), None, None, None, None, "double"),
      TypedNode(cluster.st1, "running_time", None, Some(132L), None, None, None, None, None, "long"),
      TypedNode(cluster.leia, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.leia, "name", Some("Princess Leia"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.lucas, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.lucas, "name", Some("George Lucas"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.irvin, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.irvin, "name", Some("Irvin Kernshner"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw1, "dgraph.type", Some("Film"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw1, "title", Some("Star Wars: Episode IV - A New Hope"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw1, "title@en", Some("Star Wars: Episode IV - A New Hope"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw1, "title@hu", Some("Csillagok háborúja IV: Egy új remény"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw1, "title@be", Some("Зорныя войны. Эпізод IV: Новая надзея"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw1, "title@cs", Some("Star Wars: Epizoda IV – Nová naděje"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw1, "title@br", Some("Star Wars Lodenn 4: Ur Spi Nevez"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw1, "title@de", Some("Krieg der Sterne"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw1, "release_date", None, None, None, Some(Timestamp.valueOf("1977-05-25 00:00:00.0")), None, None, None, "timestamp"),
      TypedNode(cluster.sw1, "revenue", None, None, Some(7.75E8), None, None, None, None, "double"),
      TypedNode(cluster.sw1, "running_time", None, Some(121L), None, None, None, None, None, "long"),
      TypedNode(cluster.sw2, "dgraph.type", Some("Film"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw2, "title", Some("Star Wars: Episode V - The Empire Strikes Back"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw2, "title@en", Some("Star Wars: Episode V - The Empire Strikes Back"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw2, "title@ka", Some("ვარსკვლავური ომები, ეპიზოდი V: იმპერიის საპასუხო დარტყმა"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw2, "title@ko", Some("제국의 역습"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw2, "title@iw", Some("מלחמת הכוכבים - פרק 5: האימפריה מכה שנית"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw2, "title@de", Some("Das Imperium schlägt zurück"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw2, "release_date", None, None, None, Some(Timestamp.valueOf("1980-05-21 00:00:00.0")), None, None, None, "timestamp"),
      TypedNode(cluster.sw2, "revenue", None, None, Some(5.34E8), None, None, None, None, "double"),
      TypedNode(cluster.sw2, "running_time", None, Some(124L), None, None, None, None, None, "long"),
      TypedNode(cluster.luke, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.luke, "name", Some("Luke Skywalker"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.han, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.han, "name", Some("Han Solo"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.richard, "dgraph.type", Some("Person"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.richard, "name", Some("Richard Marquand"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw3, "dgraph.type", Some("Film"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw3, "title", Some("Star Wars: Episode VI - Return of the Jedi"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw3, "title@en", Some("Star Wars: Episode VI - Return of the Jedi"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw3, "title@zh", Some("星際大戰六部曲：絕地大反攻"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw3, "title@th", Some("สตาร์ วอร์ส เอพพิโซด 6: การกลับมาของเจได"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw3, "title@fa", Some("بازگشت جدای"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw3, "title@ar", Some("حرب النجوم الجزء السادس: عودة الجيداي"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw3, "title@de", Some("Die Rückkehr der Jedi-Ritter"), None, None, None, None, None, None, "string"),
      TypedNode(cluster.sw3, "release_date", None, None, None, Some(Timestamp.valueOf("1983-05-25 00:00:00.0")), None, None, None, "timestamp"),
      TypedNode(cluster.sw3, "revenue", None, None, Some(5.72E8), None, None, None, None, "double"),
      TypedNode(cluster.sw3, "running_time", None, Some(131L), None, None, None, None, None, "long"),
    )

  def getExpectedWideNodeSchema: StructType = StructType(Seq(
    StructField("subject", LongType, nullable = false),
    StructField("dgraph.type", StringType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("release_date", TimestampType, nullable = true),
    StructField("revenue", DoubleType, nullable = true),
    StructField("running_time", LongType, nullable = true),
    StructField("title", StringType, nullable = true)
  ))

  def getExpectedWideNodeDf(spark: SparkSession): DataFrame =
    spark.createDataset(getExpectedWideNodes.toSeq)(RowEncoder(getExpectedWideNodeSchema)).toDF()

  def getExpectedWideNodes: Set[Row] =
    Set(
      Row(cluster.st1, "Film", null, Timestamp.valueOf("1979-12-07 00:00:00.0"), 1.39E8, 132L, null),
      Row(cluster.leia, "Person", "Princess Leia", null, null, null, null),
      Row(cluster.lucas, "Person", "George Lucas", null, null, null, null),
      Row(cluster.irvin, "Person", "Irvin Kernshner", null, null, null, null),
      Row(cluster.sw1, "Film", null, Timestamp.valueOf("1977-05-25 00:00:00.0"), 7.75E8, 121L, "Star Wars: Episode IV - A New Hope"),
      Row(cluster.sw2, "Film", null, Timestamp.valueOf("1980-05-21 00:00:00.0"), 5.34E8, 124L, "Star Wars: Episode V - The Empire Strikes Back"),
      Row(cluster.luke, "Person", "Luke Skywalker", null, null, null, null),
      Row(cluster.han, "Person", "Han Solo", null, null, null, null),
      Row(cluster.richard, "Person", "Richard Marquand", null, null, null, null),
      Row(cluster.sw3, "Film", null, Timestamp.valueOf("1983-05-25 00:00:00.0"), 5.72E8, 131L, "Star Wars: Episode VI - Return of the Jedi")
    )

}
