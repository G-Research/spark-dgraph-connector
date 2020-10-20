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

package org.apache.spark.sql.util

import java.util

import org.apache.spark.sql.sources.v2.DataSourceOptions
import scala.collection.JavaConverters._

class CaseInsensitiveStringMap(map: java.util.Map[String, String]) extends java.util.Map[String, String] {

  val options: util.Map[String, String] = map.asScala.map(e => (toLowerCase(e._1), e._2)).asJava

  import java.util.Locale

  private def toLowerCase(key: Any): String = key.toString.toLowerCase(Locale.ROOT)

  override def size(): Int = options.size()

  override def isEmpty: Boolean = options.isEmpty

  override def containsKey(o: Any): Boolean = options.containsKey(toLowerCase(o))

  override def containsValue(o: Any): Boolean = options.containsValue(o)

  override def get(o: Any): String = options.get(toLowerCase(o))

  override def put(k: String, v: String): String = throw new UnsupportedOperationException("DataSourceOptions are readonly")

  override def remove(o: Any): String = throw new UnsupportedOperationException("DataSourceOptions are readonly")

  override def putAll(map: util.Map[_ <: String, _ <: String]): Unit = throw new UnsupportedOperationException("DataSourceOptions are readonly")

  override def clear(): Unit = throw new UnsupportedOperationException("DataSourceOptions are readonly")

  override def keySet(): util.Set[String] = options.keySet()

  override def values(): util.Collection[String] = options.values()

  override def entrySet(): util.Set[util.Map.Entry[String, String]] = options.entrySet()

}

object CaseInsensitiveStringMap {

  implicit class CaseInsensitiveDataSourceOptions(options: DataSourceOptions)
    extends CaseInsensitiveStringMap(options.asMap())

  def empty(): CaseInsensitiveStringMap = new CaseInsensitiveStringMap(new util.HashMap[String, String]())

}
