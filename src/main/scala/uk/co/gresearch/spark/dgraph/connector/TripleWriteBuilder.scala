package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.connector.write.{BatchWrite, WriteBuilder}
import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel

case class TripleWriteBuilder(schema: StructType, model: GraphTableModel)
  extends WriteBuilder {
  override def buildForBatch(): BatchWrite = TripleBatchWrite(schema, model)
}
