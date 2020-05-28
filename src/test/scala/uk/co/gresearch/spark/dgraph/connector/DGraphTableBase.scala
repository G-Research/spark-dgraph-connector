package uk.co.gresearch.spark.dgraph.connector

import java.util

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import scala.collection.JavaConverters._

trait DGraphTableBase extends Table with SupportsRead{

  val targets: Seq[Target]

  private lazy val tableName = targets.map(_.toString).mkString(",")

  override def name(): String = tableName

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

}
