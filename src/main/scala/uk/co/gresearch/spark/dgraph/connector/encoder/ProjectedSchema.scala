package uk.co.gresearch.spark.dgraph.connector.encoder

import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.Predicate

trait ProjectedSchema { this: InternalRowEncoder =>

  /**
   * The projected schema.
   */
  val projectedSchema: Option[StructType]

  /**
   * Returns the predicates actually read from Dgraph. These are the predicates that correspond to readSchema
   * if projected schema is given to this encoder. Note: This preserves order of projected schema.
   */
  val readPredicates: Option[Seq[Predicate]]

}
