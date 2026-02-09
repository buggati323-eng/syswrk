package uk.ac.ntu.cloudfs.common.scheduler;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public final class EmaLatencyScheduler implements Scheduler {
    @Override
    public String name() { return "ema_latency"; }

    @Override
    public Optional<NodeInfo> pick(List<NodeInfo> nodes) {
        if (nodes == null || nodes.isEmpty()) return Optional.empty();
        return nodes.stream()
                .min(Comparator.comparingLong(n -> {
                    long v = n.emaLatencyMs();
                    return v == 0 ? Long.MAX_VALUE : v; // prefer nodes with real measurements
                }));
    }
}