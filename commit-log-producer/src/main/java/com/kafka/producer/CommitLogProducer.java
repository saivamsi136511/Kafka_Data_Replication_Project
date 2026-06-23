package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

@CommandLine.Command(
    name = "commit-log-producer",
    mixinStandardHelpOptions = true,
    version = "1.0.0",
    description = "Produces synthetic JSON commit-log events to a Kafka topic."
)
public class CommitLogProducer implements Callable<Integer> {
    private static final Logger log = LoggerFactory.getLogger(CommitLogProducer.class);

    @CommandLine.Option(names = {"--count", "-n"}, description = "Number of events to produce (required).", required = true)
    private int count;

    @CommandLine.Option(names = {"--bootstrap-server", "-b"}, description = "Kafka broker address. Default: localhost:9092", defaultValue = "localhost:9092")
    private String bootstrapServer;

    @CommandLine.Option(names = {"--topic", "-t"}, description = "Target topic name. Default: commit-log", defaultValue = "commit-log")
    private String topic;

    @CommandLine.Option(names = {"--batch-size"}, description = "Flush after this many records. Default: 50", defaultValue = "50")
    private int batchSize;

    @CommandLine.Option(names = {"--async"}, description = "Send without waiting for broker ACK. Higher throughput, less durability guarantee at app layer.")
    private boolean asyncMode;

    @CommandLine.Option(names = {"--max-retries"}, description = "Retry attempts per message on failure. Default: 3", defaultValue = "3")
    private int maxRetries;

    @CommandLine.Option(names = {"--acks"}, description = "Broker acknowledgement level. Values: all, 1, 0. Default: all", defaultValue = "all")
    private String acks;

    private long lastOffset = -1L;
    private int failureCount = 0;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new CommitLogProducer()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        printHeader();
        log.info("Starting CommitLogProducer: count={}, topic={}, bootstrap={}, async={}, batchSize={}", 
            count, topic, bootstrapServer, asyncMode, batchSize);

        EventGenerator generator = new EventGenerator();
        ProgressTracker tracker = new ProgressTracker(count);

        try (KafkaProducer<String, String> producer = createProducer()) {
            for (int i = 0; i < count; ++i) {
                String eventJson = generator.generateEvent(i + 1L);
                String messageKey = generator.extractMessageKey(eventJson);
                boolean sent = sendWithRetry(producer, messageKey, eventJson, i + 1);

                if (sent) {
                    tracker.recordSuccess(eventJson.getBytes(StandardCharsets.UTF_8).length);
                } else {
                    tracker.recordFailure();
                    failureCount++;
                    log.warn("Permanently failed to send event #{} after {} retries", i + 1, maxRetries);
                }

                if ((i + 1) % batchSize == 0) {
                    producer.flush();
                    log.debug("Flushed batch at event #{}", i + 1);
                }
            }
            producer.flush();
            tracker.finish();
            printSummary();
            return failureCount == 0 ? 0 : 1;
        } catch (Exception e) {
            log.error("Fatal error during event production: {}", e.getMessage(), e);
            System.err.println("Fatal error: " + e.getMessage());
            return 2;
        }
    }

    private boolean sendWithRetry(KafkaProducer<String, String> producer, String key, String value, int eventNum) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        for (int attempt = 1; attempt <= maxRetries; ++attempt) {
            try {
                if (asyncMode) {
                    producer.send(record, (metadata, ex) -> {
                        if (ex != null) {
                            log.error("Async send failed for event #{}: {}", eventNum, ex.getMessage());
                        } else {
                            lastOffset = metadata.offset();
                            log.trace("Event #{} sent -> partition={}, offset={}", eventNum, metadata.partition(), metadata.offset());
                        }
                    });
                    return true;
                }

                Future<RecordMetadata> future = producer.send(record);
                RecordMetadata meta = future.get();
                lastOffset = meta.offset();
                log.trace("Event #{} sent -> partition={}, offset={}", eventNum, meta.partition(), meta.offset());
                return true;
            } catch (Exception e) {
                long backoffMs = 100L * (1L << (attempt - 1));
                log.warn("Attempt {}/{} failed for event #{}: {}. Waiting {} ms before retry.", 
                    attempt, maxRetries, eventNum, e.getMessage(), backoffMs);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return false;
    }

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", acks);
        props.put("enable.idempotence", "true");
        props.put("retries", String.valueOf(Integer.MAX_VALUE));
        props.put("max.in.flight.requests.per.connection", "1");
        props.put("linger.ms", "5");
        props.put("compression.type", "lz4");
        props.put("buffer.memory", "33554432");
        props.put("request.timeout.ms", "15000");
        props.put("delivery.timeout.ms", "60000");
        props.put("client.id", "commit-log-producer-v1");
        log.info("Creating KafkaProducer -> bootstrap={}, acks={}, idempotent=true", bootstrapServer, acks);
        return new KafkaProducer<>(props);
    }

    private void printHeader() {
        System.out.println();
        System.out.println("=================================================");
        System.out.println("  Kafka Commit Log Producer  v1.0");
        System.out.println("  Data Replication Project - Primary Cluster");
        System.out.println("=================================================");
        System.out.println("  Topic          : " + topic);
        System.out.println("  Bootstrap      : " + bootstrapServer);
        System.out.println("  Total events   : " + count);
        System.out.println("  Send mode      : " + (asyncMode ? "ASYNC" : "SYNC"));
        System.out.println("  Batch flush    : every " + batchSize + " records");
        System.out.println("=================================================");
        System.out.println();
    }

    private void printSummary() {
        System.out.println();
        System.out.println("Last confirmed offset : " + lastOffset);
        System.out.println("Failed sends          : " + failureCount);
        System.out.println();
        if (failureCount == 0) {
            System.out.println("All events produced successfully.");
            System.out.println("Verify with: kafka-console-consumer.sh --topic " + topic + " --from-beginning");
        } else {
            System.out.println("Warning: " + failureCount + " events failed. Check the log file for details.");
        }
    }
}
