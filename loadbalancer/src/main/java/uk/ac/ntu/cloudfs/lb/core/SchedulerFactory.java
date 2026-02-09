package uk.ac.ntu.cloudfs.lb.core;

import uk.ac.ntu.cloudfs.common.scheduler.EmaLatencyScheduler;
import uk.ac.ntu.cloudfs.common.scheduler.FcfsScheduler;
import uk.ac.ntu.cloudfs.common.scheduler.LeastInFlightScheduler;
import uk.ac.ntu.cloudfs.common.scheduler.PriorityScheduler;
import uk.ac.ntu.cloudfs.common.scheduler.RoundRobinScheduler;
import uk.ac.ntu.cloudfs.common.scheduler.Scheduler;

public final class SchedulerFactory {
    private SchedulerFactory() {}

    public static Scheduler create(String name) {
        if (name == null) return new RoundRobinScheduler();
        return switch (name.trim().toLowerCase()) {
            case "fcfs" -> new FcfsScheduler();
            case "priority" -> new PriorityScheduler();

            case "least_in_flight" -> new LeastInFlightScheduler();
            case "ema_latency" -> new EmaLatencyScheduler();
            case "round_robin" -> new RoundRobinScheduler();
            default -> new RoundRobinScheduler();
        };
    };
    
}
    
