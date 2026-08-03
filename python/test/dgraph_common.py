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

        import subprocess

        # Start 'mvn' as a separate process and capture its stdin and stdout
        cls.dgraph = subprocess.Popen(
            ['mvn', '--batch-mode', '-Dspotless.check.skip', '-DskipTests', '-Dmaven.test.skip=true', 'exec:java', '-Dexec.classpathScope=test', '-Dexec.mainClass=uk.co.gresearch.spark.dgraph.DgraphTestCluster', f'-Dexec.args={DgraphClusterTest.get_pom_path()} false'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        # determine target and local ip target of dgraph instance
        cls.dgraph.target = None
        cls.dgraph.targetLocalIp = None
        line = cls.dgraph.stdout.readline()
        while line:
            print(line, end='')
            if line.startswith("target="):
                cls.dgraph.target = line.split("=")[1].strip()
            if line.startswith("target-local-ip="):
                cls.dgraph.targetLocalIp = line.split("=")[1].strip()

            if line.strip() == "Dgraph cluster is running. Press ENTER to stop.":
                if cls.dgraph.target is None or cls.dgraph.targetLocalIp is None:
                    raise RuntimeError("Could not determine target or local ip of Dgraph instance")
                break
            line = cls.dgraph.stdout.readline()

    @classmethod
    def tearDownClass(cls):
        logging.info('stopping Dgraph')
        cls.dgraph.communicate(input='\n')
        super(DgraphClusterTest, cls).tearDownClass()
