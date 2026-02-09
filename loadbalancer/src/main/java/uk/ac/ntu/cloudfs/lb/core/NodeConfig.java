package uk.ac.ntu.cloudfs.lb.core;

import java.util.ArrayList;
import java.util.List;

import uk.ac.ntu.cloudfs.common.scheduler.NodeInfo;

public final class NodeConfig {
    private NodeConfig() {}

    /**
 * LB_NODES format:
 *   node-1=http://localhost:9001,node-2=http://localhost:9002
 * Optional priority:
 *   node-1:10=http://localhost:9001,node-2:0=http://localhost:9002
 */
    public static List<NodeInfo> fromEnv() {
        String raw = System.getenv("LB_NODES");
        if (raw == null || raw.isBlank()) {
            List<NodeInfo> defs = new ArrayList<>();
            defs.add(new NodeInfo("node-1", "http://localhost:9001"));
            defs.add(new NodeInfo("node-2", "http://localhost:9002"));
            defs.add(new NodeInfo("node-3", "http://localhost:9003"));
            defs.add(new NodeInfo("node-4", "http://localhost:9004"));
            return defs;
        }

        List<NodeInfo> nodes = new ArrayList<>();
        for (String part : raw.split(",")) {
            String p = part.trim();
            if (p.isEmpty()) continue;

            String[] kv = p.split("=", 2);
            if (kv.length != 2) continue;

            String idRaw = kv[0].trim();
            String url = kv[1].trim();
            if (idRaw.isEmpty() || url.isEmpty()) continue;

            int prio = 0;
            String id = idRaw;

            int idx = idRaw.lastIndexOf(':');
            if (idx > 0 && idx < idRaw.length() - 1) {
                String tail = idRaw.substring(idx + 1);
                try {
                    prio = Integer.parseInt(tail.trim());
                    id = idRaw.substring(0, idx).trim();
                } catch (NumberFormatException ignored) {
                // keep default prio=0, id as full string
                }
            }

            if (!id.isEmpty()) {
                nodes.add(new NodeInfo(id, url, prio));
            }
        }

        // fallback if parsing produced nothing
        if (nodes.isEmpty()) {
            nodes.add(new NodeInfo("node-1", "http://localhost:9001"));
        }

        return nodes;
    }
}