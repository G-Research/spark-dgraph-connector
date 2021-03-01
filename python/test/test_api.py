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

import py4j
import unittest

from spark_common import SparkTest
from gresearch.spark.dgraph import connector


class ApiTest(SparkTest):
    pass


# create a test for each constant in the API
def create_test(const, value):
    def test(self):
        jvm = self.spark._jvm
        pkg = jvm.uk.co.gresearch.spark.dgraph.connector.package

        try:
            expected = getattr(pkg, const)()
        except py4j.protocol.Py4JError as e:
            self.fail(e)
        self.assertEqual(value, expected)

    return test


api = connector
# all attributes of the connector package
attrs = [(key, getattr(api, key)) for key in api.__dict__.keys() if not key.startswith('__')]
# all constants of the connector package (strings, ints, floats and booleans)
consts = [(key, attr) for (key, attr) in attrs if type(attr) in [str, int, float, bool]]

# create a test method for each constant
for (const, value) in consts:
    method = create_test(const, value)
    method.__name__ = 'test_api_constant_{}'.format(const)
    setattr(ApiTest, method.__name__, method)


if __name__ == '__main__':
    unittest.main()
