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

package uk.co.gresearch.spark.dgraph.connector.partitioner

import org.scalatest.FunSpec
import uk.co.gresearch.spark.dgraph.connector.{Partition, Target}

class TestSingletonPartitioner extends FunSpec {

  describe("SingletonPartitioner") {

    val targets = Seq(Target("host1:9080"), Target("host2:9080"))

    it("should partition") {
      val partitioner = SingletonPartitioner(targets)
      val partitions = partitioner.getPartitions

      assert(partitions.length === 1)
      assert(partitions.toSet === Set(Partition(targets, None, None, None)))
    }

  }

}
