package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.{DataFrame, Encoders, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.StructType

case class TripleRelation(sqlContext: SQLContext, schema: StructType) extends BaseRelation with InsertableRelation {
  override def insert(df: DataFrame, overwrite: Boolean): Unit = {
    df.show()

    import sqlContext.implicits._
    df.groupBy($"subject").as[Long, Row](Encoders.scalaLong, df.encoder)
      .flatMapGroups { (uid, it) => it }(df.encoder)
  }
}
