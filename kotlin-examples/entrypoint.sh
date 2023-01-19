#!/bin/bash

set -eu

echo "Overriding CBS entrypoint as this gives us declarative control"

# Creating directories
mkdir -p /opt/couchbase/var/lib/couchbase/{config,data,stats,logs}

echo "Run up Couchbase Server"
/opt/couchbase/bin/couchbase-server -- -kernel global_enable_tracing false -noinput &

echo "Wait for it to be ready"
until curl -sS -w 200 http://127.0.0.1:8091/ui/index.html &> /dev/null; do
    echo "Not ready, waiting to recheck"
    sleep 2
done

echo "Configuring cluster"
couchbase-cli cluster-init -c 127.0.0.1 \
    --cluster-username Administrator \
    --cluster-password password \
    --services data,index,query,fts,analytics \
    --cluster-ramsize 1024 \
    --cluster-index-ramsize 512 \
    --cluster-eventing-ramsize 512 \
    --cluster-fts-ramsize 256 \
    --cluster-analytics-ramsize 1024 \
    --index-storage-setting default

echo "Creating main bucket"
couchbase-cli bucket-create -c 127.0.0.1 \
  --username Administrator \
  --password password \
  --bucket travel-sample \
  --bucket-type couchbase \
  --bucket-ramsize 100 \
  --bucket-replica 1 \

echo "Creating index bucket"
couchbase-cli bucket-create -c 127.0.0.1 \
  --username Administrator \
  --password password \
  --bucket travel-sample-index \
  --bucket-type couchbase \
  --bucket-ramsize 100 \
  --bucket-replica 1 \

echo "Enable audit logging"
couchbase-cli setting-audit -c 127.0.0.1 \
    --username Administrator \
    --password password \
    --set \
    --audit-enabled 1

echo "Waiting for startup completion"
# Wait for startup - no great way for this
until curl -u "Administrator:password" http://127.0.0.1:8091/pools/default &> /dev/null; do
    echo "Not running, waiting to recheck"
    sleep 2
done

echo "Running"
# Ensure everyone can read the logs as new ones are created
until ! chmod -R a+r /opt/couchbase/var/lib/couchbase/logs/; do
    sleep 10
done

echo "Exiting"
