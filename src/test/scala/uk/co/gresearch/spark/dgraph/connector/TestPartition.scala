package uk.co.gresearch.spark.dgraph.connector

import org.scalatest.FunSpec

class TestPartition extends FunSpec with SchemaProvider {

  describe("Partition") {

    Seq(1, 3, 10, 30, 100, 300, 1000, 3000, 10000).foreach { predicates =>

      // test that a partition works with N predicates
      // the query grows linearly with N, so does the processing time
      it(s"should read $predicates predicates") {
        val targets = Seq(Target("localhost:9080"))
        val existingPredicates = getSchema(targets).predicates.slice(0, predicates)
        val syntheticPredicates =
          (1 to (predicates - existingPredicates.size)).map(pred =>
            (s"predicate$pred", if (pred % 2 == 0) "string" else "uid")
          ).toMap
        val schema = Schema(syntheticPredicates ++ existingPredicates)
        val partition = Partition(targets, schema)
        println(partition.getTriples.length)
      }

    }

  }

}
