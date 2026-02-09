package uk.ac.ntu.cloudfs.common.scheduler;

import java.util.List;
import java.util.Optional;

public interface Scheduler {
    String name();

    
    Optional<NodeInfo> pick(List<NodeInfo> nodes);
}