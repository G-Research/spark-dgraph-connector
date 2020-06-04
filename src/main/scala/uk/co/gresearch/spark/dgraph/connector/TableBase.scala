package uk.co.gresearch.spark.dgraph.connector

import java.util
import java.util.UUID

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}

import scala.collection.JavaConverters._

trait TableBase extends Table with SupportsRead{

  val cid: UUID

  override def name(): String = s"dgraph cluster ($cid)"

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

}
