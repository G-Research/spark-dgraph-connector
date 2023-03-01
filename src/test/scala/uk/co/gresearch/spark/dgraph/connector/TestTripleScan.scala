package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.connector.expressions.{NamedReference, Transform}
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning
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
      val targets = Seq(Target("localhost:1234"))
      val partitions = Seq(Partition(targets), Partition(targets))
      val scan = TripleScan(TestPartitioner(partitions, Some(clusterColumns)), model)
      val partitioning = scan.outputPartitioning()

      describe(s"with [${clusterColumns.mkString(",")}]") {
        it("should report KeyGroupedPartitioning") {
          assert(partitioning.isInstanceOf[KeyGroupedPartitioning])
          val kgPartitioning = partitioning.asInstanceOf[KeyGroupedPartitioning]

          assert(kgPartitioning.numPartitions() === partitions.length)
          assert(kgPartitioning.keys().length === clusterColumns.length)
          assert(kgPartitioning.keys().forall(_.isInstanceOf[Transform]))
          val args = kgPartitioning.keys().map(_.asInstanceOf[Transform].arguments())

          assert(args.forall(_.forall(_.isInstanceOf[NamedReference])))
          args.map(_.flatMap(_.asInstanceOf[NamedReference].fieldNames()))
            .zip(clusterColumns)
            .foreach { case (keys: Array[String], column: String) => assert(keys === Array(column)) }
        }
      }

    }
  }

}
