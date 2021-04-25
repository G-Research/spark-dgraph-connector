# keep env var name in-sync with uk.co.gresearch.spark.dgraph.DgraphTestCluster.DgraphVersionEnvVar
# keep default value in-sync with uk.co.gresearch.spark.dgraph.DgraphTestCluster.DgraphDefaltVersion
version=${DGRAPH_TEST_CLUSTER_VERSION:-21.03.0}

if [[ $version < "21.03.0" ]]
then
  whitelist="--whitelist=0.0.0.0/0"
else
  whitelist="--security whitelist=0.0.0.0/0"
fi

docker run --rm -d -p 8080:8080 -p 9080:9080 -p 8000:8000 -p 6080:6080 -v "$(cd "$(dirname "$0")"; pwd)/dgraph-instance:/dgraph" dgraph/dgraph:v${version} /bin/bash -c "dgraph zero & dgraph alpha $whitelist"
