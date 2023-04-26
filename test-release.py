from pyspark import SparkConf
from pyspark.sql import SparkSession

# noinspection PyUnresolvedReferences
from gresearch.spark.dgraph.connector import *

conf = SparkConf().setAppName('integration test').setMaster('local[2]')
conf = conf.setAll([
    ('spark.ui.showConsoleProgress', 'false'),
    ('spark.locality.wait', '0'),
])

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()

spark.read.dgraph.triples("localhost:9080").show()

