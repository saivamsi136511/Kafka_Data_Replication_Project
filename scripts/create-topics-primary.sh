#!/bin/bash
# =============================================================================
# create-topics-primary.sh
# Runs inside the primary Kafka container on first startup.
# Creates the commit-log topic with the required retention settings.
# =============================================================================

set -e

BOOTSTRAP="localhost:9092"
TOPIC="commit-log"

echo "[Topic Init] Waiting for Kafka to be ready..."
sleep 5

echo "[Topic Init] Creating topic: $TOPIC"
kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=60000 \
  --config segment.ms=60000 \
  --config cleanup.policy=delete

echo "[Topic Init] Topic '$TOPIC' created with retention.ms=60000"

# Verify
kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --describe \
  --topic "$TOPIC"

echo "[Topic Init] Done."
