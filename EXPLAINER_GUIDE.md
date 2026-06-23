# Explainer Guide: Kafka Data Replication Project

This guide provides a comprehensive overview of the **Kafka Data Replication Project** (Tasks 1, 2, and 3) so that you can easily understand and explain the architecture, code changes, and verification process to others.

---

## 1. Project Overview & Architecture

### The Scenario
We are simulating a mission-critical Write-Ahead Log (WAL) replication pipeline:
* **Primary Cluster (PR)** runs on port `9092`. Applications produce state-changing operations (such as INSERT, UPDATE, DELETE) to a `commit-log` topic.
* **MirrorMaker 2 (MM2)** replicates this stream cross-cluster to a **Standby Cluster (DR)** running on port `9093`, writing to the `primary.commit-log` topic.
* **DR Services** read from the replicated topic to keep their state in sync.

### The Failures We Solve
1. **Silent Data Loss (Task 2)**: Vanilla MM2 silently skips offsets deleted by retention policies, causing data gaps in DR. We detect this and fail-fast.
2. **Service Disruption (Task 3)**: Planned maintenance operations involving topic reset (topic deletion and recreation) can cause the replication service to stall or crash. We handle this by auto-resubscribing from the beginning offset.

---

## 2. Key Components & File Names

### A. Task 1: Commit Log Producer CLI (Local Workspace)
Located under `commit-log-producer/`. It is a Java application built with Maven:
* [`CommitLogProducer.java`](commit-log-producer/src/main/java/com/kafka/producer/CommitLogProducer.java): Entry point. Configures CLI arguments (bootstrap server, record count, sync/async modes) using PicoCLI.
* [`EventGenerator.java`](commit-log-producer/src/main/java/com/kafka/producer/EventGenerator.java): Generates realistic, stateful JSON events representing system transitions (e.g., `INSERT` -> `UPDATE` -> `DELETE`) to mirror real application logs.
* [`ProgressTracker.java`](commit-log-producer/src/main/java/com/kafka/producer/ProgressTracker.java): Renders an in-place console progress bar displaying throughput (events/sec), % complete, and ETA.
* [`logback.xml`](commit-log-producer/src/main/resources/logback.xml): Routes application logs cleanly (stderr for CLI logs so they don't break stdout progress bar, and debug log file).

### B. Tasks 2 & 3: MirrorMaker 2 Enhancements (Kafka Repository)
Located in `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java` in the forked Kafka repository (see GitHub link in README):
* **This is the core class in MirrorMaker 2 that reads messages from the source topic and replicates them to the target.**
* We added custom methods and fields under a designated section at the end of the file (under 120 lines of code).

---

## 3. Code Modifications & Why We Made Them

Here is a breakdown of our enhancements inside `MirrorSourceTask.java` (in the forked Kafka repository):

### 1. Truncation Gap Detection & Fail-Fast (Task 2)
* **What we did**: Added `checkForLogTruncation()` which is called proactively at the start of `poll()`.
* **Why**: By comparing the consumer's current position (`consumer.position(partition)`) with the broker's earliest available offset (`beginningOffsets()`), we can detect if the consumer has been outpaced by the retention boundary.
* **Action**: If `earliestAvailable > currentPosition`, it means messages were deleted before they could be replicated. We throw a custom `DataLossTruncationException` (fail-fast) to stop replication, preventing a silent data gap in the DR cluster.

### 2. Truncation Audit Logging (Innovation B)
* **What we did**: Before throwing the exception, we write a structured JSON record describing the truncation event to the `_mm2_audit_log` topic on the standby cluster.
* **Why**: In real production, operators need to know exactly which partitions had data loss, when, and how many messages were skipped.
* **Code**:
  ```java
  String auditRecord = String.format(
      "{\"event_type\":\"TRUNCATION_DETECTED\",\"source_topic\":\"%s\",\"partition\":%d,\"expected_offset\":%d,\"earliest_available\":%d,\"estimated_lost\":%d,\"detected_at\":\"%s\"}",
      partition.topic(), partition.partition(), expected, earliest, lost, Instant.now().toString()
  );
  ```

### 3. Early Warning System (Innovation A)
* **What we did**: If the consumer is within 1000 offsets of the retention boundary but hasn't been outpaced yet, we log a warning.
* **Why**: Gives operators a proactive alert *before* data loss actually occurs, so they can take preventive action.

### 4. Topic Reset & Auto-Resubscription (Task 3)
* **What we did**: Added exception handling in `poll()` for `OffsetOutOfRangeException` and `UnknownTopicOrPartitionException`, and proactive checks in `checkForLogTruncation()` where `position > latestAvailable`.
* **Why**: When a topic is deleted and recreated, the broker's offsets reset to 0, making the consumer's expected offset invalid.
* **Action**: We intercept these errors, log the reset details along with partition and timestamps, and call `consumer.seek(partition, beginningOffset)` to seek back to `0` and continue replication automatically.

### 5. Systemic Issue Escalation (Innovation C)
* **What we did**: Configured a `resetCounter` and `lastResetTime` window.
* **Why**: If a topic is reset repeatedly (e.g. 3 times in 5 minutes), it indicates a systemic scripting issue, configuration loop, or security event rather than normal maintenance.
* **Action**: On the 3rd reset, we escalate the warning log to a high-severity `ERROR` log requesting manual operator review.

---

## 4. How the Pipeline is Deployed

We build the modified Kafka Connect mirror library and package it into a custom Docker image:

1. **Gradle Build**: We compile the Connect mirror JAR inside the Kafka codebase:
   ```bash
   java -cp gradle/wrapper/gradle-wrapper.jar org.gradle.wrapper.GradleWrapperMain :connect:mirror:jar
   ```
2. **Dockerfile**: Copy our newly compiled `connect-mirror-4.0.0.jar` into `/opt/kafka/libs/` inside the official `apache/kafka:4.0.0` Docker image to overwrite the default library:
   ```dockerfile
   FROM apache/kafka:4.0.0
   COPY connect-mirror-4.0.0.jar /opt/kafka/libs/connect-mirror-4.0.0.jar
   ```
3. **Properties (`mm2.properties`)**: Configured MM2 with internal connection ports (`29092`) and explicitly set the replication factors to `1` for the internal config, offsets, and status topics. This ensures it starts successfully on single-node test brokers without waiting for a multi-node quorum.

---

## 5. Summary Checklist of What to Go Through

To explain this project successfully, you should be familiar with these files and sections:

1. **Main Entry Points**:
   * `CommitLogProducer.java` (`commit-log-producer/src/main/java/com/kafka/producer/CommitLogProducer.java`): Notice how it handles parameters and sets up safe producer configs (`acks=all`, idempotence).
   * `MirrorSourceTask.java` line 147 (`poll`): Look at the try-catch block wrapping `consumer.poll()` and the call to `checkForLogTruncation()`.
2. **Custom Logic Functions**:
   * `MirrorSourceTask.java` line 300 (`checkForLogTruncation`): Proactive checks and early warnings.
   * `MirrorSourceTask.java` line 362 (`triggerTruncationFailure`): Writes the audit log and throws the exception to crash.
   * `MirrorSourceTask.java` line 400 (`handleTopicReset`): Escalates resets and seeks the consumer to the beginning.
3. **Configuration**:
   * [`docker-compose.yml`](docker-compose.yml): The containers orchestration mapping host ports `9092`/`9093` to internal network ports.
   * [`mm2.properties`](mirrormaker2/mm2.properties): Overrides for replication factors and flow selection.
