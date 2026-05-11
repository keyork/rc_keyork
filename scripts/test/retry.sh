#!/usr/bin/env bash
# Manually retry a failed notification.
# Usage: ./scripts/test/retry.sh <notification_id> [base_url]

ID=${1:?usage: retry.sh <notification_id>}
BASE=${2:-http://localhost:8080}

curl -s -X POST "$BASE/api/v1/notifications/$ID/retry" | jq .
