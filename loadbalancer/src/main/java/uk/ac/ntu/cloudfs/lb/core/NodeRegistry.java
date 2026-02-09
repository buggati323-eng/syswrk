package uk.ac.ntu.cloudfs.lb.core;

import uk.ac.ntu.cloudfs.common.scheduler.NodeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public final class NodeRegistry {
    private final CopyOnWriteArrayList<NodeInfo> nodes = new CopyOnWriteArrayList<>();

    public void addNode(NodeInfo node) { nodes.add(node); }

    public List<NodeInfo> all() { return new ArrayList<>(nodes); }

    public List<NodeInfo> healthy() {
        return nodes.stream().filter(NodeInfo::healthy).collect(Collectors.toList());
    }
}