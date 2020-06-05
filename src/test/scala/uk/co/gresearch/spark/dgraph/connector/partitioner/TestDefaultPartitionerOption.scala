package uk.co.gresearch.spark.dgraph.connector.partitioner

import java.util.UUID

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Predicate, Schema, Target}

import scala.collection.JavaConverters._

class TestDefaultPartitionerOption extends FunSpec {

  describe("DefaultPartitionerOption") {
    val target = Target("localhost:9080")
    val noTargets = Seq.empty[Target]
    val singleTarget = Seq(target)
    val twoTargets = Seq(target, target)
    val schema = Schema(Set(Predicate("pred", "string")))
    val state = ClusterState(
      Map("1" -> singleTarget.toSet),
      Map("1" -> schema.predicates.map(_.predicateName)),
      10000,
      UUID.randomUUID()
    )
    val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)

    Seq(
      (noTargets, schema, state, options, "no targets"),
      (singleTarget, schema, state, options, "single target"),
      (twoTargets, schema, state, options, "two targets"),
    ).foreach{ case (targets, schema, state, options, label) =>

      it(s"should always provide a partitioner - $label") {
        new DefaultPartitionerOption().getPartitioner(targets, schema, state, options)
      }

    }
  }
}
