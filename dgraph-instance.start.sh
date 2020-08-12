# keep env var name in-sync with uk.co.gresearch.spark.dgraph.DgraphTestCluster.DgraphVersionEnvVar
# keep default value in-sync with uk.co.gresearch.spark.dgraph.DgraphTestCluster.DgraphDefaltVersion
version=${DGRAPH_TEST_CLUSTER_VERSION:-20.03.4}
docker run --rm -it -p 8080:8080 -p 9080:9080 -p 8000:8000 -p 6080:6080 -v "$(cd "$(dirname "$0")"; pwd)/dgraph-instance:/dgraph" dgraph/dgraph:v${version} /bin/bash -c "dgraph-ratel & dgraph zero & dgraph alpha --lru_mb 1024 --whitelist 0.0.0.0/0"
