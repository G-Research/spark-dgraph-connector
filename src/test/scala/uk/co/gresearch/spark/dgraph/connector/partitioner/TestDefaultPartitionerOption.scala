package uk.co.gresearch.spark.dgraph.connector.partitioner

import java.util.UUID

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.{ClusterState, Predicate, Schema, Target}

import scala.collection.JavaConverters._

class TestDefaultPartitionerOption extends FunSpec {

  describe("DefaultPartitionerOption") {
    val target = Target("localhost:9080")
    val schema = Schema(Set(Predicate("pred", "string")))
    val state = ClusterState(
      Map("1" -> Set(target)),
      Map("1" -> schema.predicates.map(_.predicateName)),
      10000,
      UUID.randomUUID()
    )
    val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)

    it(s"should provide a partitioner") {
      new DefaultPartitionerOption().getPartitioner(schema, state, options)
    }
  }
}
