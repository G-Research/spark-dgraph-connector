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

import org.apache.spark.sql.types.StructType
import uk.co.gresearch.spark.dgraph.connector.Predicate

trait ProjectedSchema { this: InternalRowEncoder =>

  /**
   * The projected schema.
   */
  val projectedSchema: Option[StructType]

  /**
   * Returns the predicates actually read from Dgraph. These are the predicates that correspond to readSchema if
   * projected schema is given to this encoder. Note: This preserves order of projected schema.
   */
  val readPredicates: Option[Seq[Predicate]]

}
