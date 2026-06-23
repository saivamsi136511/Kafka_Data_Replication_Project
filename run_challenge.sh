#!/usr/bin/env bash
# =============================================================================
# run_challenge.sh
# Automated replication pipeline test harness.
# Demonstrates Normal Replication, Log Truncation (Task 2), and Topic Reset (Task 3).
# =============================================================================

set -eo pipefail

# Disable POSIX path conversion in Git Bash on Windows
export MSYS_NO_PATHCONV=1

# ANSI color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Helper function to wait for a pattern to appear in docker logs
wait_for_log() {
    local container=$1
    local pattern=$2
    local timeout=$3
    local elapsed=0
    
    echo -n -e "${YELLOW}Waiting for '$pattern' in $container logs...${NC}"
    while [ $elapsed -lt $timeout ]; do
        # Dump logs to a file to avoid SIGPIPE failure under set -o pipefail
        docker logs "$container" > ./logs/docker_logs_temp.txt 2>&1 || true
        if grep -q "$pattern" ./logs/docker_logs_temp.txt; then
            echo -e " ${GREEN}[FOUND]${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
        elapsed=$((elapsed + 2))
    done
    echo -e " ${RED}[TIMEOUT]${NC}"
    return 1
}

echo -e "${CYAN}===================================================================${NC}"
echo -e "${CYAN}             Kafka Data Replication Challenge Test Harness         ${NC}"
echo -e "${CYAN}===================================================================${NC}"

# Check for Docker Compose
if ! command -v docker compose &> /dev/null; then
    echo -e "${RED}[ERROR] docker compose is not installed.${NC}"
    exit 1
fi

# Cleanup previous runs
echo -e "${YELLOW}[Cleanup] Resetting environment...${NC}"
docker compose down -v --remove-orphans

# Start Kafka clusters and wait for setup to complete
echo -e "${YELLOW}[Setup] Starting Kafka clusters...${NC}"
docker compose up -d primary-kafka standby-kafka

echo -e "${YELLOW}[Setup] Waiting for Kafka health checks...${NC}"
docker compose port primary-kafka 9092 >/dev/null
docker compose port standby-kafka 9093 >/dev/null

# Rebuild and start MirrorMaker 2
echo -e "${YELLOW}[Setup] Rebuilding MirrorMaker 2 image...${NC}"
docker build -t vamsi511/enhanced-mirrormaker2:1.0.0 ./mirrormaker2
docker compose up -d mirrormaker2

# Run kafka-setup to ensure topics exist
echo -e "${YELLOW}[Setup] Initializing topics...${NC}"
docker compose up kafka-setup

echo -e "${GREEN}[Setup] Kafka environment successfully initialized!${NC}"
echo -e "${CYAN}-------------------------------------------------------------------${NC}"

# =============================================================================
# Scenario 1: Normal Flow
# =============================================================================
echo -e "${YELLOW}[Scenario 1] Verifying normal cross-cluster replication...${NC}"

# Produce 1000 events using fat JAR producer
echo -e "${YELLOW}[Scenario 1] Producing 1000 events to primary cluster...${NC}"
java -jar commit-log-producer/target/commit-log-producer-1.0.0.jar --count 1000 --bootstrap-server localhost:9092

# Wait for replication to sync
echo -e "${YELLOW}[Scenario 1] Waiting for MirrorMaker 2 to replicate...${NC}"
sleep 10

# Consume events from DR standby cluster
echo -e "${YELLOW}[Scenario 1] Consuming events from standby cluster...${NC}"
DR_COUNT=$(docker exec standby-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic primary.commit-log \
  --from-beginning \
  --max-messages 1000 \
  --timeout-ms 5000 | grep -c "event_id" || true)

echo -e "Primary events produced: 1000"
echo -e "Standby events replicated: $DR_COUNT"

if [ "$DR_COUNT" -eq 1000 ]; then
    echo -e "${GREEN}[SUCCESS] Scenario 1: Normal replication verified successfully!${NC}"
else
    echo -e "${RED}[FAILURE] Scenario 1: Replicated count ($DR_COUNT) does not match expected 1000.${NC}"
    exit 1
fi
echo -e "${CYAN}-------------------------------------------------------------------${NC}"

# =============================================================================
# Scenario 2: Log Truncation & Fail-Fast (Task 2)
# =============================================================================
# IMPORTANT: We reset the environment here so MM2 starts fresh with a known
# low offset (~50). After Scenario 1, MM2's position was ~1000, so deleting
# to offset 75 would have no effect (75 < 1000 = no gap). By resetting, we
# ensure: MM2 position = 50, delete to 75 → gap of 25 messages. ✓
# =============================================================================
echo -e "${YELLOW}[Scenario 2] Resetting environment for clean truncation test...${NC}"
docker compose down -v
docker compose up -d primary-kafka standby-kafka
docker compose up kafka-setup
docker compose up -d mirrormaker2

# Wait for MM2 to connect and start tracking the topic
wait_for_log mirrormaker2 "refreshing topics" 90 || true
sleep 5

# Produce 50 messages — MM2 replicates all of them, saving position ~50
echo -e "${YELLOW}[Scenario 2] Producing 50 seed messages (MM2 position will be ~50)...${NC}"
java -jar commit-log-producer/target/commit-log-producer-1.0.0.jar --count 50 --bootstrap-server localhost:9092

# Give MM2 time to fully consume and commit offsets for those 50 messages
echo -e "${YELLOW}[Scenario 2] Waiting for MM2 to commit offset ~50...${NC}"
sleep 10

# Stop MM2 — its committed consumer offset is now frozen at ~50
echo -e "${YELLOW}[Scenario 2] Freezing MirrorMaker 2 at offset ~50...${NC}"
docker compose stop mirrormaker2

# Produce 50 more messages — primary log now has offsets ~50 to ~100
echo -e "${YELLOW}[Scenario 2] Producing 50 more events to primary (offsets ~50-100)...${NC}"
java -jar commit-log-producer/target/commit-log-producer-1.0.0.jar --count 50 --bootstrap-server localhost:9092

# Delete records up to offset 75:
#   MM2 will resume at offset ~50, but the log now starts at 75.
#   Gap = 75 - 50 = 25 messages → DataLossTruncationException WILL fire.
echo -e "${YELLOW}[Scenario 2] Simulating log truncation: deleting primary records up to offset 75...${NC}"
docker exec primary-kafka sh -c "echo '{\"partitions\": [{\"topic\": \"commit-log\", \"partition\": 0, \"offset\": 75}], \"version\": 1}' > /tmp/delete-spec-challenge.json"
docker exec primary-kafka /opt/kafka/bin/kafka-delete-records.sh --bootstrap-server localhost:29092 --offset-json-file /tmp/delete-spec-challenge.json

# Restart MM2 — it resumes at ~50, sees earliest = 75, detects gap → fail-fast
echo -e "${YELLOW}[Scenario 2] Restarting MirrorMaker 2 to trigger gap detection...${NC}"
docker compose start mirrormaker2

# Verify MM2 threw DataLossTruncationException
echo -e "${YELLOW}[Scenario 2] Scanning MirrorMaker 2 logs for DataLossTruncationException...${NC}"
if wait_for_log mirrormaker2 "DataLossTruncationException" 90; then
    echo -e "${GREEN}[SUCCESS] Scenario 2: MirrorMaker 2 successfully failed-fast on truncation gap!${NC}"
else
    echo -e "${RED}[FAILURE] Scenario 2: MirrorMaker 2 did not crash or raise DataLossTruncationException.${NC}"
    exit 1
fi

# Verify the audit log on the standby cluster
echo -e "${YELLOW}[Scenario 2] Verifying audit log topic on standby cluster...${NC}"
AUDIT_LOG=$(docker exec standby-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic _mm2_audit_log \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 5000 || true)

if echo "$AUDIT_LOG" | grep -q "TRUNCATION_DETECTED"; then
    echo -e "${GREEN}[SUCCESS] Scenario 2: JSON audit log generated correctly: ${CYAN}$AUDIT_LOG${NC}"
else
    echo -e "${RED}[FAILURE] Scenario 2: Standby cluster did not receive the TRUNCATION_DETECTED audit log.${NC}"
    exit 1
fi
echo -e "${CYAN}-------------------------------------------------------------------${NC}"

# =============================================================================
# Scenario 3: Topic Reset & Auto-Resubscribe (Task 3)
# =============================================================================
echo -e "${YELLOW}[Scenario 3] Verifying topic reset auto-resubscription and reset counter escalation...${NC}"

# 1. Clean the environment to clear the truncation failure state
docker compose down -v
docker compose up -d primary-kafka standby-kafka mirrormaker2
docker compose up kafka-setup

# Wait for MirrorMaker 2 to finish initialization
wait_for_log mirrormaker2 "refreshing topics" 90 || true

echo -e "${YELLOW}[Scenario 3] Running initial production...${NC}"
java -jar commit-log-producer/target/commit-log-producer-1.0.0.jar --count 10 --bootstrap-server localhost:9092

# Wait for replication to begin on the new topic
wait_for_log mirrormaker2 "commit-log-0" 60 || true
sleep 5

# 2. Reset the topic by deleting it (1st reset)
echo -e "${YELLOW}[Scenario 3] Deleting topic commit-log on primary (1st Reset)...${NC}"
docker exec primary-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic commit-log

# Check if MM2 handled the reset and automatically resubscribed
if wait_for_log mirrormaker2 "Topic reset detected" 60; then
    echo -e "${GREEN}[SUCCESS] Scenario 3: Topic reset detected proactively by MirrorMaker 2!${NC}"
else
    echo -e "${RED}[FAILURE] Scenario 3: Topic reset warning not found in logs.${NC}"
    exit 1
fi

# 3. Simulate two more resets to trigger escalation
echo -e "${YELLOW}[Scenario 3] Triggering 2nd and 3rd resets to test escalation...${NC}"

# 2nd Reset
java -jar commit-log-producer/target/commit-log-producer-1.0.0.jar --count 1 --bootstrap-server localhost:9092
sleep 3
docker exec primary-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic commit-log
wait_for_log mirrormaker2 "Topic reset detected" 60 || true

# 3rd Reset
java -jar commit-log-producer/target/commit-log-producer-1.0.0.jar --count 1 --bootstrap-server localhost:9092
sleep 3
docker exec primary-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic commit-log

# Check for systemic issue error alert in logs
echo -e "${YELLOW}[Scenario 3] Scanning MirrorMaker 2 logs for SYSTEMIC ISSUE escalation error...${NC}"
if wait_for_log mirrormaker2 "SYSTEMIC ISSUE" 60; then
    echo -e "${GREEN}[SUCCESS] Scenario 3: Repeated reset escalated to SYSTEMIC ISSUE error successfully!${NC}"
else
    echo -e "${RED}[FAILURE] Scenario 3: MirrorMaker 2 did not escalate repeated resets to ERROR.${NC}"
    exit 1
fi

echo -e "${CYAN}===================================================================${NC}"
echo -e "${GREEN}      All scenarios completed successfully and verified!           ${NC}"
echo -e "${CYAN}===================================================================${NC}"
