package uk.co.gresearch.spark

import org.apache.spark.sql.SparkSession

trait SparkTest {

  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[1]")
      .appName("spark test example")
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.local.dir", ".")
      .getOrCreate()
  }

}
