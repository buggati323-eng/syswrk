package uk.ac.ntu.cloudfs.lb.core;

import uk.ac.ntu.cloudfs.common.scheduler.NodeInfo;
import uk.ac.ntu.cloudfs.common.scheduler.Scheduler;

import java.util.ArrayList;
import java.util.List;

public final class ReplicaPicker {
    private ReplicaPicker() {}

    public static List<NodeInfo> pick(List<NodeInfo> healthy, Scheduler scheduler, int replicas) {
        if (healthy == null || healthy.isEmpty() || replicas <= 0) return List.of();

        int n = Math.min(replicas, healthy.size());
        List<NodeInfo> pool = new ArrayList<>(healthy);
        List<NodeInfo> out = new ArrayList<>(n);

        for (int i = 0; i < n; i++) {
            var pick = scheduler.pick(pool);
            if (pick.isEmpty()) break;
            NodeInfo chosen = pick.get();
            out.add(chosen);
            pool.remove(chosen);
        }
        return out;
    }

    public static List<NodeInfo> pickTwo(List<NodeInfo> healthy, Scheduler scheduler) {
        return pick(healthy, scheduler, 2);
    }
}