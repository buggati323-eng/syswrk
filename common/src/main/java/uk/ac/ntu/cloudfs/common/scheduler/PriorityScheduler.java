package uk.ac.ntu.cloudfs.common.scheduler;

import java.util.List;
import java.util.Optional;

public final class PriorityScheduler implements Scheduler {
    @Override public String name() { return "priority"; }

    @Override
    public Optional<NodeInfo> pick(List<NodeInfo> nodes) {
        if (nodes == null || nodes.isEmpty()) return Optional.empty();

        NodeInfo best = nodes.get(0);
        for (int i = 1; i < nodes.size(); i++) {
            NodeInfo n = nodes.get(i);

            if (n.priority() > best.priority()) {
                best = n;
                continue;
            }
            if (n.priority() < best.priority()) continue;

            // same priority -> pick the one with fewer in-flight requests
            if (n.inFlight() < best.inFlight()) best = n;
        }
        return Optional.of(best);
    }
}