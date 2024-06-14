/*
 * Copyright 2020 G-Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector.encoder

trait NoColumnInfo extends ColumnInfo {

  /**
   * Indicates whether the column name refers to the subject column. Must be implemented to support filter pushdown.
   * @param columnName
   *   column name
   * @return
   *   true if column is the subject
   */
  override def isSubjectColumn(columnName: String): Boolean = false

  /**
   * Indicates whether the column name refers to the predicate column. Must be implemented to support filter pushdown.
   * @param columnName
   *   column name
   * @return
   *   true if column is the predicate
   */
  override def isPredicateColumn(columnName: String): Boolean = false

  /**
   * Indicates whether the column name refers to a specific predicate and its (object) value. Must be implemented to
   * support filter pushdown.
   * @param columnName
   *   column name
   * @return
   *   true if column is a predicate's value
   */
  def isPredicateValueColumn(columnName: String): Boolean = false

  /**
   * Indicates whether the column name refers to the object type. Must be implemented to support filter pushdown.
   * @param columnName
   *   column name
   * @return
   *   true if column is the object type
   */
  override def isObjectTypeColumn(columnName: String): Boolean = false

  /**
   * Indicates whether the column name refers to the object value. Must be implemented to support filter pushdown.
   * @param columnName
   *   column name
   * @return
   *   true if column is the object value
   */
  override def isObjectValueColumn(columnName: String): Boolean = false

  /**
   * Provides the type of the given object value column. Must be implemented to support filter pushdown.
   * @param columnName
   *   column name
   * @return
   *   object type
   */
  override def getObjectType(columnName: String): Option[String] = None

}
