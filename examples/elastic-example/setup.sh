#!/bin/sh
set -e

echo "Waiting for Elasticsearch..."
until curl -s -o /dev/null -w "%{http_code}" -u elastic:${ELASTIC_PASSWORD} http://elasticsearch:9200/ | grep -q "200"; do
  echo "Not ready, retrying in 5s..."
  sleep 5
done

echo "Setting kibana_system password..."
until curl -s -o /dev/null -w "%{http_code}" \
  -X POST \
  -u elastic:${ELASTIC_PASSWORD} \
  -H "Content-Type: application/json" \
  http://elasticsearch:9200/_security/user/kibana_system/_password \
  -d "{\"password\":\"${KIBANA_PASSWORD}\"}" | grep -q "200"; do
  echo "Retrying in 5s..."
  sleep 5
done

echo "Done."