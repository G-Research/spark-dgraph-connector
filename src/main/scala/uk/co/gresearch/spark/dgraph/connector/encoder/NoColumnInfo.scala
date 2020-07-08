package uk.co.gresearch.spark.dgraph.connector.encoder

trait NoColumnInfo extends ColumnInfo {

  /**
   * Indicates whether the column name refers to the subject column.
   * Must be implemented to support filter pushdown.
   * @param columnName column name
   * @return true if column is the subject
   */
  override def isSubjectColumn(columnName: String): Boolean = false

  /**
   * Indicates whether the column name refers to the predicate column.
   * Must be implemented to support filter pushdown.
   * @param columnName column name
   * @return true if column is the predicate
   */
  override def isPredicateColumn(columnName: String): Boolean = false

  /**
   * Indicates whether the column name refers to a specific predicate and its (object) value.
   * Must be implemented to support filter pushdown.
   * @param columnName column name
   * @return true if column is a predicate's value
   */
  def isPredicateValueColumn(columnName: String): Boolean = false

  /**
   * Indicates whether the column name refers to the object type.
   * Must be implemented to support filter pushdown.
   * @param columnName column name
   * @return true if column is the object type
   */
  override def isObjectTypeColumn(columnName: String): Boolean = false

  /**
   * Indicates whether the column name refers to the object value.
   * Must be implemented to support filter pushdown.
   * @param columnName column name
   * @return true if column is the object value
   */
  override def isObjectValueColumn(columnName: String): Boolean = false

  /**
   * Provides the type of the given object value column.
   * Must be implemented to support filter pushdown.
   * @param columnName column name
   * @return object type
   */
  override def getObjectType(columnName: String): Option[String] = None

}
