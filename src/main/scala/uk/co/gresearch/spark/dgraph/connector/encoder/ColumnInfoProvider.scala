package uk.co.gresearch.spark.dgraph.connector.encoder

trait ColumnInfoProvider extends ColumnInfo {

  def subjectColumnName: Option[String]
  def predicateColumnName: Option[String]
  def objectTypeColumnName: Option[String]
  def objectValueColumnNames: Option[Set[String]]
  def objectTypes: Option[Map[String,String]]

  /**
   * Indicates whether the column name refers to the subject column.
   * Must be implemented to support filter push down.
   * @param columnName column name
   * @return true if column is the subject
   */
  override def isSubjectColumn(columnName: String): Boolean = subjectColumnName.exists(_.equals(columnName))

  /**
   * Indicates whether the column name refers to the predicate column.
   * Must be implemented to support filter push down.
   * @param columnName column name
   * @return true if column is the predicate
   */
  override def isPredicateColumn(columnName: String): Boolean = predicateColumnName.exists(_.equals(columnName))

  /**
   * Indicates whether the column name refers to the object type.
   * Must be implemented to support filter push down.
   * @param columnName column name
   * @return true if column is the object type
   */
  override def isObjectTypeColumn(columnName: String): Boolean = objectTypeColumnName.exists(_.equals(columnName))

  /**
   * Indicates whether the column name refers to the object value.
   * Must be implemented to support filter push down.
   * @param columnName column name
   * @return true if column is the object value
   */
  override def isObjectValueColumn(columnName: String): Boolean = objectValueColumnNames.exists(_.contains(columnName))

  /**
   * Provides the type of the given object value column.
   * Must be implemented to support filter push down.
   * @param columnName column name
   * @return object type
   */
  override def getObjectType(columnName: String): Option[String] = objectTypes.flatMap(_.get(columnName))

}
