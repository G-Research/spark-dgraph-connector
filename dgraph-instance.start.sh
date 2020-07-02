docker run --rm -it -p 8080:8080 -p 9080:9080 -p 8000:8000 -p 6080:6080 -v "$(cd "$(dirname "$0")"; pwd)/dgraph-instance:/dgraph" dgraph/standalone:v20.03.3
