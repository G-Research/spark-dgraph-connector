package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.connector.read.partitioning.{ClusteredDistribution, Distribution}
import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.connector.encoder.StringTripleEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.{ExecutorProvider, JsonGraphQlExecutor}
import uk.co.gresearch.spark.dgraph.connector.model.TestModel
import uk.co.gresearch.spark.dgraph.connector.partitioner.Partitioner

class TestTripleScan extends AnyFunSpec {

  private val executor = new JsonGraphQlExecutor {
    override def query(query: GraphQl): Json = Json("{}")
  }

  private val executionProvider = new ExecutorProvider {
    override def getExecutor(partition: Partition): JsonGraphQlExecutor = executor
  }

  private val rowEncoder = StringTripleEncoder(Map.empty)
  private val model = TestModel(executionProvider, rowEncoder, 1000)

  case class TestPartitioner(partitions: Seq[Partition], partitionColumns: Option[Seq[String]]) extends Partitioner {
    override def configOption: String = "test"

    override def getPartitions: Seq[Partition] = partitions

    override def getPartitionColumns: Option[Seq[String]] = partitionColumns
  }

  describe("TripleScan") {
    Seq(Seq("col"), Seq("col1", "col2"), Seq("col1", "col2", "col3")).foreach { clusterColumns =>
      val scan = TripleScan(TestPartitioner(Seq(), Some(clusterColumns)), model)
      val partitioning = scan.outputPartitioning()

      describe(s"with [${clusterColumns.mkString(",")}]") {
        it(s"should not satisfy unknown distribution") {
          assert(partitioning.satisfy(new Distribution() {}) === false)
        }

        it(s"should not satisfy clustered distribution with unknown column: [col0]") {
          assert(partitioning.satisfy(new ClusteredDistribution(Seq("col0").toArray)) === false)
        }

        if (clusterColumns.nonEmpty) {
          it(s"should satisfy identical clustered distribution: ${clusterColumns.mkString(",")}") {
            assert(partitioning.satisfy(new ClusteredDistribution(clusterColumns.toArray)))
          }
        }

        val partitioningColumns = clusterColumns :+ "col0"
        it(s"should satisfy clustered distribution with more columns: [${partitioningColumns.mkString(",")}]") {
          assert(partitioning.satisfy(new ClusteredDistribution(partitioningColumns.toArray)))
        }

        it(s"should not satisfy partially overlapping clustered distribution: [${partitioningColumns.mkString(",")}]") {
          val scan = TripleScan(TestPartitioner(Seq(), Some(partitioningColumns)), model)
          val partitioning = scan.outputPartitioning()
          assert(partitioning.satisfy(new ClusteredDistribution(("colX" +: clusterColumns).toArray)) === false)
        }
      }

    }
  }

}
