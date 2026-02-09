package uk.ac.ntu.cloudfs.common.scheduler;

import java.util.List;
import java.util.Optional;

public final class FcfsScheduler implements Scheduler {

    @Override
    public String name() {
        return "fcfs";
    }

    @Override
    public Optional<NodeInfo> pick(List<NodeInfo> nodes) {
        if (nodes == null || nodes.isEmpty()) return Optional.empty();
        return Optional.of(nodes.get(0));
    }
}