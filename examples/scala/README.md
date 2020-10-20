# Examples

Run these examples as follows:

    (cd ../../; mvn install)
    mvn package
    ${SPARK_HOME}/bin/spark-submit --packages uk.co.gresearch.spark:spark-dgraph-connector_2.12:0.5.0-3.0-SNAPSHOT,graphframes:graphframes:0.8.0-spark3.0-s_2.12 --class uk.co.gresearch.spark.dgraph.connector.example.ExampleApp examples/scala/target/spark-dgraph-connector-examples_2.12-*.jar
