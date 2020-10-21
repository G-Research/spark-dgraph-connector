#  Copyright 2020 G-Research
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrameReader
from pyspark.sql.dataframe import DataFrame

TriplesSource: str = 'uk.co.gresearch.spark.dgraph.triples'
EdgesSource: str = 'uk.co.gresearch.spark.dgraph.edges'
NodesSource: str = 'uk.co.gresearch.spark.dgraph.nodes'


TargetOption: str = "dgraph.target"
TargetsOption: str = "dgraph.targets"

TriplesModeOption: str = "dgraph.triples.mode"
TriplesModeStringOption: str = "string"
TriplesModeTypedOption: str = "typed"

NodesModeOption: str = "dgraph.nodes.mode"
NodesModeTypedOption: str = "typed"
NodesModeWideOption: str = "wide"

ChunkSizeOption: str = "dgraph.chunkSize"
ChunkSizeDefault: int = 100000

PartitionerOption: str = "dgraph.partitioner"
SingletonPartitionerOption: str = "singleton"
GroupPartitionerOption: str = "group"
AlphaPartitionerOption: str = "alpha"
PredicatePartitionerOption: str = "predicate"
UidRangePartitionerOption: str = "uid-range"
PartitionerDefault: str = '+'.join([PredicatePartitionerOption, UidRangePartitionerOption])

AlphaPartitionerPartitionsOption: str = "dgraph.partitioner.alpha.partitionsPerAlpha"
AlphaPartitionerPartitionsDefault: int = 1
PredicatePartitionerPredicatesOption: str = "dgraph.partitioner.predicate.predicatesPerPartition"
PredicatePartitionerPredicatesDefault: int = 1000
UidRangePartitionerUidsPerPartOption: str = "dgraph.partitioner.uidRange.uidsPerPartition"
UidRangePartitionerUidsPerPartDefault: int = 1000000
UidRangePartitionerEstimatorOption: str = "dgraph.partitioner.uidRange.estimator"
MaxLeaseIdEstimatorOption: str = "maxLeaseId"
UidRangePartitionerEstimatorDefault: str = MaxLeaseIdEstimatorOption


class DgraphReader:
    def __init__(self, reader: DataFrameReader):
        super().__init__()
        self._jvm = reader._spark._jvm
        self._spark = reader._spark
        self._reader = self._jvm.uk.co.gresearch.spark.dgraph.connector.DgraphReader(reader._jreader)

    def _toSeq(self, list) -> JavaObject:
        array = self._jvm.java.util.ArrayList(list)
        return self._jvm.scala.collection.JavaConverters.asScalaIteratorConverter(array.iterator()).asScala().toSeq()

    def triples(self, target, *targets) -> DataFrame:
        jdf = self._reader.triples(target, self._toSeq(targets))
        return DataFrame(jdf, self._spark)

    def nodes(self, target, *targets) -> DataFrame:
        jdf = self._reader.nodes(target, self._toSeq(targets))
        return DataFrame(jdf, self._spark)

    def edges(self, target, *targets) -> DataFrame:
        jdf = self._reader.edges(target, self._toSeq(targets))
        return DataFrame(jdf, self._spark)


@property
def dgraph(self) -> DgraphReader:
    return DgraphReader(self)


DataFrameReader.dgraph = dgraph
