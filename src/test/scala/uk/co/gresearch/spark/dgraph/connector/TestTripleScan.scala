package uk.co.gresearch.spark.dgraph.connector

import org.scalatest.funspec.AnyFunSpec
import uk.co.gresearch.spark.dgraph.connector.encoder.JsonNodeInternalRowEncoder
import uk.co.gresearch.spark.dgraph.connector.executor.ExecutorProvider
import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel
import uk.co.gresearch.spark.dgraph.connector.partitioner.Partitioner

class TestTripleScan extends AnyFunSpec {

  describe("TripleScan") {
    val model =
    it("should support identical distributions") {
      val partitions = Seq()
      val scan = TripleScan(TestPartitioner(partitions, Some(Seq("predicate"))), model)
      scan.outputPartitioning()
    }
  }

  case class TestModel() extends GraphTableModel {
    override val execution: ExecutorProvider = _
    override val encoder: JsonNodeInternalRowEncoder = _
    override val chunkSize: Int = _
    override val metrics: PartitionMetrics = _

    override def withMetrics(metrics: PartitionMetrics): GraphTableModel = ???
  }

  case class TestPartitioner(partitions: Seq[Partition], partitionColumns: Option[Seq[String]]) extends Partitioner {
    override def configOption: String = "test"

    override def getPartitions: Seq[Partition] = partitions

    override def getPartitionColumns: Option[Seq[String]] = partitionColumns
  }
}
