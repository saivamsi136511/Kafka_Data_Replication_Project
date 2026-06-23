# =========================================================================
# Dockerfile — Commit Log Producer
#
# Multi-stage build:
#   Stage 1 (builder): Compiles the Java source with Maven
#   Stage 2 (runtime): Minimal JRE image with just the fat JAR
#
# Usage:
#   docker build -t yourname/commit-log-producer:1.0.0 .
#   docker run --rm yourname/commit-log-producer:1.0.0 \
#       --count 1000 --bootstrap-server primary-kafka:9092
# =========================================================================

# ----------------------------
# Stage 1: Build with Maven
# ----------------------------
FROM eclipse-temurin:21-jdk-alpine AS builder

WORKDIR /build

# Copy Maven wrapper and POM first (layer cache optimization)
COPY pom.xml .

# Download dependencies before copying source (better layer caching)
# The -q flag quiets the verbose Maven output
RUN apk add --no-cache maven && \
    mvn dependency:go-offline -q

# Copy source code
COPY src ./src

# Build the fat JAR (skipping tests since this is a build image)
RUN mvn clean package -DskipTests -q

# ----------------------------
# Stage 2: Minimal runtime
# ----------------------------
FROM eclipse-temurin:21-jre-alpine AS runtime

LABEL maintainer="Kafka Data Replication Project"
LABEL description="Commit Log Producer — generates synthetic WAL events to primary Kafka cluster"
LABEL version="1.0.0"

WORKDIR /app

# Copy only the fat JAR from the builder stage
COPY --from=builder /build/target/commit-log-producer-1.0.0.jar ./commit-log-producer.jar

# Create a logs directory for the file appender
RUN mkdir -p /app/logs

# Default entrypoint — all CLI args are passed through at `docker run`
ENTRYPOINT ["java", "-jar", "/app/commit-log-producer.jar"]

# Default CMD: show help if no args given
CMD ["--help"]
