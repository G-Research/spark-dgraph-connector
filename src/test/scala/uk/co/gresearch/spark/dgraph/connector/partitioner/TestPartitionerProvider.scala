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

    Seq(
      ("singleton", (p: Partitioner) => p.isInstanceOf[SingletonPartitioner]),
      ("group", (p: Partitioner) => p.isInstanceOf[GroupPartitioner]),
      ("alpha", (p: Partitioner) => p.isInstanceOf[AlphaPartitioner]),
      ("predicate", (p: Partitioner) => p.isInstanceOf[PredicatePartitioner]),
      ("unknown", (p: Partitioner) => p.isInstanceOf[SingletonPartitioner]),
    ).foreach{ case (partOption, test) =>
      it(s"should provide $partOption partitioner via option") {
        val provider = new PartitionerProvider {}
        val options = new CaseInsensitiveStringMap(Map(PartitionerOption -> partOption).asJava)
        val partitioner = provider.getPartitioner(target, schema, state, options)
        assert(test(partitioner))
      }
    }

    it(s"should provide alpha partitioner with non-default partsPerAlpha via option") {
      val provider = new PartitionerProvider {}
      val options = new CaseInsensitiveStringMap(Map(PartitionerOption -> "alpha", AlphaPartitionerPartitionsOption -> "2").asJava)
      val partitioner = provider.getPartitioner(target, schema, state, options)
      assert(partitioner.isInstanceOf[AlphaPartitioner])
    }

    it(s"should provide predicate partitioner with non-default predsPerPart via option") {
      val provider = new PartitionerProvider {}
      val options = new CaseInsensitiveStringMap(Map(PartitionerOption -> "alpha", PredicatePartitionerPredicatesOption -> "2").asJava)
      val partitioner = provider.getPartitioner(target, schema, state, options)
      assert(partitioner.isInstanceOf[AlphaPartitioner])
    }

  }

}
