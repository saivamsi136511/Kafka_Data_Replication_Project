package com.kafka.producer;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProgressTracker {
    private static final Logger log = LoggerFactory.getLogger(ProgressTracker.class);
    private static final int BAR_WIDTH = 32;
    private static final char FILLED_CHAR = '#';
    private static final char EMPTY_CHAR = '.';
    private final long total;
    private final Instant startTime;
    private final AtomicLong produced = new AtomicLong(0L);
    private final AtomicLong totalBytes = new AtomicLong(0L);
    private long lastCheckpointCount;
    private Instant lastCheckpointTime;

    public ProgressTracker(long total) {
        this.total = total;
        this.startTime = Instant.now();
        this.lastCheckpointTime = startTime;
        this.lastCheckpointCount = 0L;
        System.out.println();
    }

    public synchronized void recordSuccess(int payloadBytes) {
        long count = produced.incrementAndGet();
        totalBytes.addAndGet(payloadBytes);
        render(count);
    }

    public void recordFailure() {
        log.debug("A send attempt failed and was not retried further");
    }

    public synchronized void finish() {
        render(total);
        System.out.println();
        Duration elapsed = Duration.between(startTime, Instant.now());
        double seconds = Math.max((double) elapsed.toMillis() / 1000.0, 0.001);
        double throughput = (double) total / seconds;
        double kb = (double) totalBytes.get() / 1024.0;
        System.out.println();
        System.out.println("--- Production Complete ---");
        System.out.printf("Events produced : %,d%n", total);
        System.out.printf("Time taken      : %.2f s%n", seconds);
        System.out.printf("Avg throughput  : %.0f events/sec%n", throughput);
        System.out.printf("Total payload   : %.2f KB%n", kb);
        System.out.println("---------------------------");
    }

    private void render(long count) {
        double pct = (double) count / (double) total;
        int filled = (int) (pct * BAR_WIDTH);
        int empty = BAR_WIDTH - filled;
        Instant now = Instant.now();
        long elapsed = Duration.between(lastCheckpointTime, now).toMillis();
        double rate = 0.0;
        if (elapsed >= 500L) {
            rate = (double) (count - lastCheckpointCount) / ((double) elapsed / 1000.0);
            lastCheckpointCount = count;
            lastCheckpointTime = now;
        }
        String bar = "[" + String.valueOf(FILLED_CHAR).repeat(Math.max(0, filled)) + 
                     String.valueOf(EMPTY_CHAR).repeat(Math.max(0, empty)) + "]";
        double kb = (double) totalBytes.get() / 1024.0;
        String line = String.format("\r%s %d/%d (%.1f%%) | %5.0f evt/s | ETA: %s | %.1f KB", 
            bar, count, total, pct * 100.0, rate, computeEta(count, now), kb);
        System.out.print(line);
        System.out.flush();
    }

    private String computeEta(long count, Instant now) {
        if (count == 0L) {
            return "--:--:--";
        }
        long remaining = total - count;
        long elapsedMs = Duration.between(startTime, now).toMillis();
        double avgRate = (double) count / Math.max((double) elapsedMs / 1000.0, 0.001);
        long etaSec = (long) ((double) remaining / avgRate);
        return String.format("%02d:%02d:%02d", etaSec / 3600L, (etaSec % 3600L) / 60L, etaSec % 60L);
    }
}
