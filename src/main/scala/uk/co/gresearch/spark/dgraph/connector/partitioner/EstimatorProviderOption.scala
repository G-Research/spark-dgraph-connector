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

package uk.co.gresearch.spark.dgraph.connector.partitioner

import com.google.common.primitives.UnsignedLong
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector._

trait EstimatorProviderOption extends ConfigParser with ClusterStateHelper with Logging {

  def getEstimatorOption(option: String, options: CaseInsensitiveStringMap, default: String,
                         clusterState: ClusterState): UidCardinalityEstimator = {
    val name = getStringOption(option, options, default)
    name match {
      case MaxUidEstimatorOption =>
        val maxUid = getIntOption(MaxUidEstimatorIdOption, options).map(UnsignedLong.valueOf(_))
        maxUid.foreach(id => log.warn(s"using configured $MaxUidEstimatorIdOption=$id for uid cardinality estimator"))
        UidCardinalityEstimator.forMaxUid(maxUid.orElse(clusterState.maxUid))
      case _ => throw new IllegalArgumentException(s"Unknown uid cardinality estimator: $name")
    }
  }

}
