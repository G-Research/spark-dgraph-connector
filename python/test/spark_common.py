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

import contextlib
import logging
import os
import subprocess
import unittest

from pyspark import SparkConf
from pyspark.sql import SparkSession


@contextlib.contextmanager
def spark_session():
    from pyspark.sql import SparkSession

    session = SparkSession \
        .builder \
        .config(conf=SparkTest.conf) \
        .getOrCreate()

    try:
        yield session
    finally:
        session.stop()


class SparkTest(unittest.TestCase):

    @staticmethod
    def get_dependencies_from_mvn() -> str:
        logging.info('running mvn to get JVM dependencies')
        dependencies_process = subprocess.run(
            ['/bin/bash', '-c', 'mvn dependency:build-classpath 2>/dev/null | grep -A 1 "Dependencies classpath:$" | tail -n 1'],
            cwd='../..', stdout=subprocess.PIPE
        )
        if dependencies_process.returncode != 0:
            raise RuntimeError("failed to run mvn to get classpath for JVM")
        return str(dependencies_process.stdout.strip())

    @staticmethod
    def get_spark_config(dependencies) -> SparkConf:
        master = 'local[2]'
        conf = SparkConf().setAppName('unit test').setMaster(master)
        return conf.setAll([
            ('spark.ui.showConsoleProgress', 'false'),
            ('spark.test.home', os.environ.get('SPARK_HOME')),
            ('spark.locality.wait', '0'),
            ('spark.driver.extraClassPath', '{}'.format(':'.join([
                os.path.join(os.getcwd(), '../../target/classes'),
                os.path.join(os.getcwd(), '../../target/test-classes'),
                dependencies
            ]))),
        ])

    dependencies = get_dependencies_from_mvn.__func__()
    logging.info('found {} JVM dependencies'.format(len(dependencies.split(':'))))
    conf = get_spark_config.__func__(dependencies)
    spark: SparkSession = None

    @classmethod
    def setUpClass(cls):
        logging.info('launching Spark')

        cls.spark = SparkSession \
            .builder \
            .config(conf=cls.conf) \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        logging.info('stopping Spark')
        cls.spark.stop()
