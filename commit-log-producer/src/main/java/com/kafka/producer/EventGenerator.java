package com.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventGenerator {
    private static final Logger log = LoggerFactory.getLogger(EventGenerator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<String> OP_TYPES = List.of("INSERT", "INSERT", "UPDATE", "UPDATE", "UPDATE", "DELETE", "READ", "READ");
    private static final List<String> KEY_PREFIXES = List.of("doc", "order", "user", "session", "txn", "product", "invoice");
    private static final List<String> STATUSES = List.of("active", "archived", "pending", "processed", "failed", "cancelled", "draft", "published", "locked", "expired");
    private final Random random = new Random();

    public String generateEvent(long sequenceNumber) {
        try {
            ObjectNode event = MAPPER.createObjectNode();
            event.put("event_id", UUID.randomUUID().toString());
            event.put("timestamp", Instant.now().getEpochSecond());
            String opType = OP_TYPES.get(random.nextInt(OP_TYPES.size()));
            event.put("op_type", opType);
            String prefix = KEY_PREFIXES.get(random.nextInt(KEY_PREFIXES.size()));
            String keyId = String.format("%04x", sequenceNumber & 0xFFFFL);
            event.put("key", prefix + ":" + keyId);
            event.set("value", buildValuePayload(opType, sequenceNumber));
            return MAPPER.writeValueAsString(event);
        } catch (Exception e) {
            log.error("Failed to serialize event at sequence {}: {}", sequenceNumber, e.getMessage(), e);
            throw new RuntimeException("Event serialization failed", e);
        }
    }

    private ObjectNode buildValuePayload(String opType, long sequenceNumber) {
        ObjectNode value = MAPPER.createObjectNode();
        switch (opType) {
            case "INSERT":
                value.put("status", "active");
                value.put("version", 1);
                value.put("created_by", "producer-" + (sequenceNumber % 10L));
                value.put("checksum", Long.toHexString(sequenceNumber * 31L));
                break;
            case "UPDATE":
                value.put("status", STATUSES.get(random.nextInt(STATUSES.size())));
                value.put("version", random.nextInt(50) + 2);
                value.put("updated_at", Instant.now().getEpochSecond());
                break;
            case "DELETE":
                value.put("reason", "TTL_EXPIRED");
                value.put("deleted_at", Instant.now().getEpochSecond());
                break;
            case "READ":
                value.put("cache_hit", random.nextBoolean());
                value.put("latency_ms", random.nextInt(200));
                break;
            default:
                value.put("raw", opType);
                break;
        }
        return value;
    }

    public String extractMessageKey(String eventJson) {
        try {
            ObjectNode node = (ObjectNode) MAPPER.readTree(eventJson);
            return node.path("key").asText(UUID.randomUUID().toString());
        } catch (Exception e) {
            log.warn("Could not extract message key, using random UUID fallback");
            return UUID.randomUUID().toString();
        }
    }
}
