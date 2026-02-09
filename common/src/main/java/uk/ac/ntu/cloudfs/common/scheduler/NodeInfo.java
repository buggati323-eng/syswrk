package uk.ac.ntu.cloudfs.common.scheduler;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class NodeInfo {
    private final String nodeId;
    private final String baseUrl;

    private final int priority; // NEW

    private volatile boolean healthy = true;

    // simple live metrics
    private final AtomicInteger inFlight = new AtomicInteger(0);
    private final AtomicLong emaLatencyMs = new AtomicLong(0);

    public NodeInfo(String nodeId, String baseUrl) {
        this(nodeId, baseUrl, 0); // default priority
    }

    public NodeInfo(String nodeId, String baseUrl, int priority) {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.baseUrl = Objects.requireNonNull(baseUrl);
        this.priority = priority;
    }

    public int priority() { return priority; } // NEW

    public String nodeId() { return nodeId; }
    public String baseUrl() { return baseUrl; }

    public boolean healthy() { return healthy; }
    public void setHealthy(boolean healthy) { this.healthy = healthy; }

    public int inFlight() { return inFlight.get(); }
    public void incInFlight() { inFlight.incrementAndGet(); }
    public void decInFlight() { inFlight.decrementAndGet(); }

    public long emaLatencyMs() { return emaLatencyMs.get(); }

    
    public void recordLatencyMs(long sampleMs) {
        if (sampleMs < 0) return;
        long prev = emaLatencyMs.get();
        if (prev == 0) {
            emaLatencyMs.set(sampleMs);
            return;
        }
        long updated = (long) (prev * 0.8 + sampleMs * 0.2);
        emaLatencyMs.set(updated);
    }
}