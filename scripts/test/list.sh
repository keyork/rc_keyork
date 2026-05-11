#!/usr/bin/env bash
# List notifications with optional filters.
# Usage: ./scripts/test/list.sh [base_url] [status] [domain]

BASE=${1:-http://localhost:8080}
STATUS=${2:-}
DOMAIN=${3:-}

PARAMS=""
[[ -n "$STATUS" ]] && PARAMS+="status=$STATUS&"
[[ -n "$DOMAIN" ]] && PARAMS+="domain=$DOMAIN&"

curl -s "$BASE/api/v1/notifications?${PARAMS}page=1&size=20" | jq .
