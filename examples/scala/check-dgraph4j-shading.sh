#!/bin/bash

set -euo pipefail

base="$(cd "$(dirname "$0")"; pwd)"

# install spark-dgraph-connector
(cd "$base/../.."; mvn --batch-mode -Dspotless.check.skip -DskipTests -Dmaven.test.skip=true -Dgpg.skip install)

# extract dependency tree
(cd "$base"; mvn org.apache.maven.plugins:maven-dependency-plugin:3.7.0:tree -DoutputType=json -DoutputFile=dependency-tree.json)

# extract children of dgraph4j dependency
dependencies=$(jq '.children[] | select((.groupId == "uk.co.gresearch.spark") and (.artifactId | startswith("spark-dgraph-connector"))) | .children[] | select((.groupId == "io.dgraph") and (.artifactId == "dgraph4j")) | .children' < "$base/dependency-tree.json")

# check there are no dependencies
if [ "$dependencies" == "null" ]
then
  echo "Shaded dgraph4j dependency does not pull in any dependencies"
else
  echo "Shaded dgraph4j dependency has $(jq length <<< "$dependencies") dependencies"
  exit 1
fi
