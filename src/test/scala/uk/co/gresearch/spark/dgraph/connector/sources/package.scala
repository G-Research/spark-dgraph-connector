package uk.co.gresearch.spark.dgraph.connector

import org.apache.spark.sql.Row

package object sources {

  implicit class ExtendedRow(row: Row) {
    def *(n: Int): Seq[Row] = Seq.fill(n)(row)
    def ++(n: Int): Seq[Row] = Seq.fill(n)(row).zipWithIndex.map { case (row, idx) => Row(row.toSeq.init :+ (idx+1): _*) }
  }

}
