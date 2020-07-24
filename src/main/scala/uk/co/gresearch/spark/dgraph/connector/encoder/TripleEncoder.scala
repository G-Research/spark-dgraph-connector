/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.gresearch.spark.dgraph.connector.encoder

import com.google.gson.{JsonArray, JsonObject}
import org.apache.spark.sql.catalyst.InternalRow
import uk.co.gresearch.spark.dgraph.connector.{Json, Logging, Predicate, Uid}

import scala.collection.JavaConverters._

/**
 * Encodes triples as InternalRows from Dgraph json results.
 */
trait TripleEncoder extends JsonNodeInternalRowEncoder with Logging {

  val predicates: Map[String, Predicate]

  /**
   * Encodes the given Dgraph json result into InternalRows.
   *
   * @param result Json result
   * @return internal rows
   */
  override def fromJson(result: JsonArray): Iterator[InternalRow] =
    getNodes(result).flatMap(toTriples)

  /**
   * Encodes a node as InternalRows.
   *
   * @param node a json node to turn into triples
   * @return InternalRows
   */
  def toTriples(node: JsonObject): Iterator[InternalRow] = {
    try {
      val uidString = node.remove("uid").getAsString
      val uid = Uid(uidString)
      node
        .entrySet()
        .iterator().asScala
        .flatMap(e =>
          predicates
            .get(e.getKey)
            .map(_.dgraphType)
            .map(t => (e.getKey, e.getValue, t))
        )
        .flatMap { case (p, v, t) =>
          getValues(v)
            .flatMap(v =>
              asInternalRow(uid, p, getValue(v, t))
            )
        }
    } catch {
      case t: Throwable =>
        log.error(s"failed to encode node: $node")
        throw t
    }
  }

  /**
   * Encodes a triple (s, p, o) as an internal row. Returns None if triple cannot be encoded.
   *
   * @param s subject
   * @param p predicate
   * @param o object
   * @return an internal row
   */
  def asInternalRow(s: Uid, p: String, o: Any): Option[InternalRow]

}
