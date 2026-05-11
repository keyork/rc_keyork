#!/usr/bin/env bash
# Submit a notification and print the returned notification_id.
# Usage: ./scripts/test/submit.sh [base_url]

BASE=${1:-http://localhost:8080}

curl -s -X POST "$BASE/api/v1/notifications" \
  -H "Content-Type: application/json" \
  -d '{
    "target_url":    "https://httpbin.org/post",
    "method":        "POST",
    "headers":       {"Authorization": "Bearer test-token", "Content-Type": "application/json"},
    "body":          "{\"event\":\"test\",\"user_id\":\"42\"}",
    "callback_url":  "https://httpbin.org/post",
    "source_system": "test-client"
  }' | jq .
