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

import logging

logger = logging.getLogger()
logger.level = logging.INFO

import unittest

from dgraph_common import DgraphClusterTest
from gresearch.spark.dgraph.connector import *


class DgraphTest(DgraphClusterTest):

    @classmethod
    def get_expected_triples(cls):
        jvm = cls.spark._jvm
        return jvm.uk.co.gresearch.spark.dgraph.connector.sources.TriplesSourceExpecteds(cls._jdgraph)

    @classmethod
    def get_expected_typed_triples(cls):
        jdf = cls.get_expected_triples().getExpectedTypedTripleDf(cls.spark._jsparkSession)
        df = DataFrame(jdf, cls.spark._wrapped)
        return df.collect()

    @classmethod
    def get_expected_string_triples(cls):
        jdf = cls.get_expected_triples().getExpectedStringTripleDf(cls.spark._jsparkSession)
        df = DataFrame(jdf, cls.spark._wrapped)
        return df.collect()

    def assertTypedTriples(self, data):
        self.assertEqual(sorted(data), sorted(DgraphTest.get_expected_typed_triples()))

    def assertStringTriples(self, data):
        self.assertEqual(sorted(data), sorted(DgraphTest.get_expected_string_triples()))

    def test_read_load_triples(self):
        self.assertTypedTriples(self.reader.format(TriplesSource).load(self.dgraph.target).collect())

    def test_read_load_typed_triples(self):
        self.assertTypedTriples(self.reader.format(TriplesSource).option(TriplesModeOption, TriplesModeTypedOption).load(self.dgraph.target).collect())

    def test_read_load_string_triples(self):
        self.assertStringTriples(self.reader.format(TriplesSource).option(TriplesModeOption, TriplesModeStringOption).load(self.dgraph.target).collect())

    def test_read_dgraph_triples(self):
        self.assertTypedTriples(self.reader.dgraph.triples(self.dgraph.target).collect())
        self.assertTypedTriples(self.reader.dgraph.triples(self.dgraph.target, self.dgraph.targetLocalIp).collect())

    def test_read_dgraph_typed_triples(self):
        self.assertTypedTriples(self.reader.option(TriplesModeOption, TriplesModeTypedOption).dgraph.triples(self.dgraph.target).collect())
        self.assertTypedTriples(self.reader.option(TriplesModeOption, TriplesModeTypedOption).dgraph.triples(self.dgraph.target, self.dgraph.targetLocalIp).collect())

    def test_read_dgraph_string_triples(self):
        self.assertStringTriples(self.reader.option(TriplesModeOption, TriplesModeStringOption).dgraph.triples(self.dgraph.target).collect())
        self.assertStringTriples(self.reader.option(TriplesModeOption, TriplesModeStringOption).dgraph.triples(self.dgraph.target, self.dgraph.targetLocalIp).collect())

    @classmethod
    def get_expected_nodes(cls):
        jvm = cls.spark._jvm
        return jvm.uk.co.gresearch.spark.dgraph.connector.sources.NodesSourceExpecteds(cls._jdgraph)

    @classmethod
    def get_expected_typed_nodes(cls):
        jdf = cls.get_expected_nodes().getExpectedTypedNodeDf(cls.spark._jsparkSession)
        df = DataFrame(jdf, cls.spark._wrapped)
        return df.collect()

    @classmethod
    def get_expected_wide_nodes(cls):
        jdf = cls.get_expected_nodes().getExpectedWideNodeDf(cls.spark._jsparkSession)
        df = DataFrame(jdf, cls.spark._wrapped)
        return df.collect()

    def assertTypedNodes(self, data):
        self.assertEqual(sorted(data), sorted(DgraphTest.get_expected_typed_nodes()))

    def assertWideNodes(self, data):
        self.assertEqual(sorted(data), sorted(DgraphTest.get_expected_wide_nodes()))

    def test_read_load_nodes(self):
        self.assertTypedNodes(self.reader.format(NodesSource).load(self.dgraph.target).collect())

    def test_read_load_typed_nodes(self):
        self.assertTypedNodes(self.reader.format(NodesSource).option(NodesModeOption, NodesModeTypedOption).load(self.dgraph.target).collect())

    def test_read_load_wide_nodes(self):
        self.assertWideNodes(self.reader.format(NodesSource).option(NodesModeOption, NodesModeWideOption).load(self.dgraph.target).collect())

    def test_read_dgraph_nodes(self):
        self.assertTypedNodes(self.reader.dgraph.nodes(self.dgraph.target).collect())
        self.assertTypedNodes(self.reader.dgraph.nodes(self.dgraph.target, self.dgraph.targetLocalIp).collect())

    def test_read_dgraph_typed_nodes(self):
        self.assertTypedNodes(self.reader.option(NodesModeOption, NodesModeTypedOption).dgraph.nodes(self.dgraph.target).collect())
        self.assertTypedNodes(self.reader.option(NodesModeOption, NodesModeTypedOption).dgraph.nodes(self.dgraph.target, self.dgraph.targetLocalIp).collect())

    def test_read_dgraph_wide_nodes(self):
        self.assertWideNodes(self.reader.option(NodesModeOption, NodesModeWideOption).dgraph.nodes(self.dgraph.target).collect())
        self.assertWideNodes(self.reader.option(NodesModeOption, NodesModeWideOption).dgraph.nodes(self.dgraph.target, self.dgraph.targetLocalIp).collect())

    @classmethod
    def get_expected_edges(cls):
        jvm = cls.spark._jvm
        return jvm.uk.co.gresearch.spark.dgraph.connector.sources.EdgeSourceExpecteds(cls._jdgraph)

    @classmethod
    def get_expected_edges(cls):
        jvm = cls.spark._jvm
        expecteds = jvm.uk.co.gresearch.spark.dgraph.connector.sources.EdgesSourceExpecteds(cls._jdgraph)
        jdf = expecteds.getExpectedEdgeDf(cls.spark._jsparkSession)
        df = DataFrame(jdf, cls.spark._wrapped)
        return df.collect()

    def assertEdges(self, data):
        self.assertEqual(sorted(data), sorted(DgraphTest.get_expected_edges()))

    def test_read_load_edges(self):
        self.assertEdges(self.reader.format(EdgesSource).load(self.dgraph.target).collect())

    def test_read_dgraph_edges(self):
        self.assertEdges(self.reader.dgraph.edges(self.dgraph.target).collect())
        self.assertEdges(self.reader.dgraph.edges(self.dgraph.target, self.dgraph.target, self.dgraph.targetLocalIp).collect())


if __name__ == '__main__':
    unittest.main()
