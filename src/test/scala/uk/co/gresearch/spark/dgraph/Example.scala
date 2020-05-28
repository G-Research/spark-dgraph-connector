package uk.co.gresearch.spark.dgraph

import org.apache.spark.sql.SparkSession
import uk.co.gresearch.spark.dgraph.connector.TargetOption

object Example {

  def main(args : Array[String]): Unit = {

    val spark: SparkSession = {
      SparkSession
        .builder()
        .master("local[1]")
        .appName("spark example")
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.local.dir", ".")
        .getOrCreate()
    }

    spark
      .read
      .format("uk.co.gresearch.spark.dgraph.connector.TripleSource")
      .option(TargetOption, "localhost:9080")
      .load()
      .show(false)
  }

}
