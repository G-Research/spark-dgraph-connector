package uk.co.gresearch.spark.dgraph.connector.partitioner

import java.util.UUID

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector._

import scala.collection.JavaConverters._

class TestPartitionerProvider extends FunSpec {

  val target = Seq(Target("localhost:9080"))
  val schema: Schema = Schema(Set(Predicate("pred", "string")))
  val state: ClusterState = ClusterState(
    Map("1" -> target.toSet),
    Map("1" -> schema.predicates.map(_.predicateName)),
    10000,
    UUID.randomUUID()
  )

  describe("PartitionerProvider") {

    val singleton = SingletonPartitioner(target)
    val group = GroupPartitioner(schema, state)
    val alpha =AlphaPartitioner(schema, state, AlphaPartitionerPartitionsDefault)
    val pred = PredicatePartitioner(schema, state, PredicatePartitionerPredicatesDefault)
    val uidRange = UidRangePartitioner(group, UidRangePartitionerFactorDefault, 10000)

    Seq(
      ("singleton", singleton),
      ("group", group),
      ("alpha", alpha),
      ("predicate", pred),

      ("uid-range", uidRange),
      ("singleton+uid-range", uidRange.copy(partitioner = singleton)),
      ("group+uid-range", uidRange),
      ("alpha+uid-range", uidRange.copy(partitioner = alpha)),
      ("predicate+uid-range", uidRange.copy(partitioner = pred)),
    ).foreach{ case (partOption, expected) =>

      it(s"should provide $partOption partitioner via option") {
        val provider = new PartitionerProvider {}
        val options = new CaseInsensitiveStringMap(Map(PartitionerOption -> partOption).asJava)
        val partitioner = provider.getPartitioner(target, schema, state, options)
        assert(partitioner === expected)
      }

    }

    it("should fail on unknown partitioner option") {
      val provider = new PartitionerProvider {}
      val options = new CaseInsensitiveStringMap(Map(PartitionerOption -> "unknown").asJava)
      assertThrows[IllegalArgumentException] {
        provider.getPartitioner(target, schema, state, options)
      }
    }

    it("should fail on unknown uidRange partitioner option") {
      val provider = new PartitionerProvider {}
      val options = new CaseInsensitiveStringMap(Map(PartitionerOption -> "unknown+uid-range").asJava)
      assertThrows[IllegalArgumentException] {
        provider.getPartitioner(target, schema, state, options)
      }
    }

    it(s"should provide alpha partitioner with non-default partsPerAlpha via option") {
      val provider = new PartitionerProvider {}
      val options = new CaseInsensitiveStringMap(Map(PartitionerOption -> "alpha", AlphaPartitionerPartitionsOption -> "2").asJava)
      val partitioner = provider.getPartitioner(target, schema, state, options)
      assert(partitioner === alpha.copy(partitionsPerAlpha = 2))
    }

    it(s"should provide predicate partitioner with non-default predsPerPart via option") {
      val provider = new PartitionerProvider {}
      val options = new CaseInsensitiveStringMap(Map(PartitionerOption -> "predicate", PredicatePartitionerPredicatesOption -> "2").asJava)
      val partitioner = provider.getPartitioner(target, schema, state, options)
      assert(partitioner === pred.copy(predicatesPerPartition = 2))
    }

    it(s"should provide uid-range partitioner with non-default factor via option") {
      val provider = new PartitionerProvider {}
      val options = new CaseInsensitiveStringMap(Map(PartitionerOption -> "uid-range", UidRangePartitionerFactorOption -> "2").asJava)
      val partitioner = provider.getPartitioner(target, schema, state, options)
      assert(partitioner === uidRange.copy(partitioningFactor = 2))
    }

    it(s"should provide alpha uid-range partitioner with non-default values via option") {
      val provider = new PartitionerProvider {}
      val options = new CaseInsensitiveStringMap(Map(
        PartitionerOption -> "alpha+uid-range",
        AlphaPartitionerPartitionsOption -> "2",
        UidRangePartitionerFactorOption -> "2",
      ).asJava)
      val partitioner = provider.getPartitioner(target, schema, state, options)
      assert(partitioner === uidRange.copy(partitioner = alpha.copy(partitionsPerAlpha = 2), partitioningFactor = 2))
    }

    it(s"should provide predicate partitioner with non-default values via option") {
      val provider = new PartitionerProvider {}
      val options = new CaseInsensitiveStringMap(Map(
        PartitionerOption -> "predicate+uid-range",
        PredicatePartitionerPredicatesOption -> "2",
        UidRangePartitionerFactorOption -> "2",
      ).asJava)
      val partitioner = provider.getPartitioner(target, schema, state, options)
      assert(partitioner === uidRange.copy(partitioner = pred.copy(predicatesPerPartition = 2), partitioningFactor = 2))
    }

  }

}
