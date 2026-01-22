package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel

case class TripleDataWriter(schema: StructType, model: GraphTableModel) extends DataWriter[InternalRow] {
  var triples = 0L

  override def write(row: InternalRow): Unit = {
    // Console.println(s"Writing row: $row")
    triples = triples + 1
  }

  override def commit(): WriterCommitMessage = {
    val msg: WriterCommitMessage = new WriterCommitMessage {
      val name: String = s"$triples triples (${Thread.currentThread().getName})"
      override def toString: String = name
    }
    Console.println(s"Committing $msg")
    msg
  }

  override def abort(): Unit = { }

  override def close(): Unit = { }
}
