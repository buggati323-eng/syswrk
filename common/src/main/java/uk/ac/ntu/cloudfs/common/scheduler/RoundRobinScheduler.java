package uk.ac.ntu.cloudfs.common.scheduler;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public final class RoundRobinScheduler implements Scheduler {
    private final AtomicInteger idx = new AtomicInteger(0);

    @Override
    public String name() { return "round_robin"; }

    @Override
    public Optional<NodeInfo> pick(List<NodeInfo> nodes) {
        if (nodes == null || nodes.isEmpty()) return Optional.empty();
        int i = Math.floorMod(idx.getAndIncrement(), nodes.size());
        return Optional.of(nodes.get(i));
    }
}