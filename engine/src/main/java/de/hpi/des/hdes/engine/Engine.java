package de.hpi.des.hdes.engine;

import de.hpi.des.hdes.engine.execution.plan.ExecutionPlan;
import de.hpi.des.hdes.engine.execution.slot.Slot;
import de.hpi.des.hdes.engine.execution.slot.SourceSlot;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.Topology;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Engine {

  private final Topology topology;
  private final ExecutionPlan plan;
  private final ExecutorService executor;

  public Engine(TopologyBuilder builder) {
    this.topology = builder.build();
    this.plan = ExecutionPlan.from(topology);
    this.executor = Executors.newCachedThreadPool();
  }

  public void run() {
    for (var s : this.plan.getSlots()) {
      this.executor.submit(s);
    }
  }

  public void addQuery(TopologyBuilder builder) {
    Topology additionalTopology = builder.build();

    // Check if we can share any sources
    // If yes, get the source slots and append them to a map
    HashMap<UUID, SourceSlot<?>> nodeIdToSourceSlotMap = new HashMap<>();
    for(Node newSource : additionalTopology.getSourceNodes()) {
      if(this.topology.getSourceNodes().contains(newSource)) {
         // We found an identical source, meaning that we can share state
         SourceSlot<?> sourceSlot = (SourceSlot<?>) this.plan.getSlotById(newSource.getNodeId());
         nodeIdToSourceSlotMap.put(newSource.getNodeId(), sourceSlot);
      }
    }
    log.debug("Identified {} Shared Sources", nodeIdToSourceSlotMap.size());
    ExecutionPlan extendPlan = ExecutionPlan.extend(additionalTopology, nodeIdToSourceSlotMap);

    // Add the slots which are not running
    // Also, add to the master execution plan & topology
    // Currently not really active since everything is chained
    for (Slot slot : extendPlan.getSlots()) {
        if(!slot.isAlreadyRunning()) {
            this.executor.submit(slot);
            this.plan.addSlot(slot);
            this.topology.addNode(additionalTopology.getNodeById(slot.getTopologyNodeId()));
        }
    }
  }

  public void shutdown() {
    this.executor.shutdownNow();
  }

}
