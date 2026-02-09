package uk.ac.ntu.cloudfs.lb.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ntu.cloudfs.common.scheduler.NodeInfo;

import java.util.List;

public final class HealthChecker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(HealthChecker.class);

    private final NodeRegistry registry;
    private final HttpPing ping;
    private final long intervalMs;

    public HealthChecker(NodeRegistry registry, long intervalMs) {
        this.registry = registry;
        this.ping = new HttpPing();
        this.intervalMs = intervalMs;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            List<NodeInfo> nodes = registry.all();

            for (NodeInfo n : nodes) {
                boolean ok = ping.isHealthy(n.baseUrl());
                boolean prev = n.healthy();
                n.setHealthy(ok);

                if (ok != prev) {
                    log.info("Health change: {} -> {}", n.nodeId(), ok ? "HEALTHY" : "UNHEALTHY");
                }
            }

            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}