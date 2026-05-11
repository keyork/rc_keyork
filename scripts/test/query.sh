#!/usr/bin/env bash
# Query a single notification by ID.
# Usage: ./scripts/test/query.sh <notification_id> [base_url]

ID=${1:?usage: query.sh <notification_id>}
BASE=${2:-http://localhost:8080}

curl -s "$BASE/api/v1/notifications/$ID" | jq .
