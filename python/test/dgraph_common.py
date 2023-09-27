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
import os

from spark_common import SparkTest


class Object(object):
    pass


class DgraphClusterTest(SparkTest):

    _jdgraph = None
    dgraph = None

    @staticmethod
    def get_pom_path() -> str:
        paths = ['.', '..', os.path.join('..', '..')]
        for path in paths:
            if os.path.exists(os.path.join(path, 'pom.xml')):
                return path
        raise RuntimeError('Could not find path to pom.xml, looked here: {}'.format(', '.join(paths)))

    @classmethod
    def setUpClass(cls):
        super(DgraphClusterTest, cls).setUpClass()
        logging.info('launching Dgraph')

        jvm = cls.spark._jvm
        cls._jdgraph = jvm.uk.co.gresearch.spark.dgraph.DgraphCluster(DgraphClusterTest.get_pom_path(), False)
        cls._jdgraph.start()

        cls.dgraph = Object()
        cls.dgraph.target = cls._jdgraph.target()
        cls.dgraph.targetLocalIp = cls._jdgraph.targetLocalIp()

    @classmethod
    def tearDownClass(cls):
        logging.info('stopping Dgraph')
        cls._jdgraph.stop()
        super(DgraphClusterTest, cls).tearDownClass()
