package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel

case class TripleBatchWrite(schema: StructType, model: GraphTableModel) extends BatchWrite {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory =
    TripleDataWriterFactory(schema, model)

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    writerCommitMessages.foreach(msg => Console.println(s"Committed $msg"))
  }

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = { }
}
