package uk.ac.ntu.cloudfs.common.scheduler;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public final class LeastInFlightScheduler implements Scheduler {
    @Override
    public String name() { return "least_in_flight"; }

    @Override
    public Optional<NodeInfo> pick(List<NodeInfo> nodes) {
        if (nodes == null || nodes.isEmpty()) return Optional.empty();
        return nodes.stream()
                .min(Comparator.comparingInt(NodeInfo::inFlight));
    }
}