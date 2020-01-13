package de.hpi.des.hdes.engine;

import de.hpi.des.hdes.engine.execution.plan.ExecutionPlan;
import de.hpi.des.hdes.engine.execution.slot.Slot;
import de.hpi.des.hdes.engine.execution.slot.SourceSlot;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.SourceNode;
import de.hpi.des.hdes.engine.graph.Topology;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Engine {

  private Topology currentTopology;
  private ExecutionPlan plan;
  private ExecutorService executor;
  private boolean isInitialized;
  @Getter
  private final ArrayList<Query> runningQueries;

  public Engine() {
    isInitialized = false;
    runningQueries = new ArrayList<>();
  }

  public void run() {
    for (var s : this.plan.getSlots()) {
      this.executor.submit(s);
    }
  }

  public synchronized void addQuery(Query query) {
      if(!isInitialized) {
          this.currentTopology = query.getTopology();
          this.plan = ExecutionPlan.from(query);
          this.executor = Executors.newCachedThreadPool();
          isInitialized = true;
          runningQueries.add(query);
      }
      else {
          // Check if we can share any sources
          // If yes, get the source slots and append them to a map
          Topology topology = query.getTopology();
          HashMap<UUID, SourceSlot<?>> nodeIdToSourceSlotMap = getSourceNodeToSlotMapping(topology);

          // Add all (remaining) non-source nodes to the topology
          for(Node node : topology.getNodes()) {
              if(!node.getClass().equals(SourceNode.class)) {
                  this.currentTopology.addNode(node);
              }
          }

          ExecutionPlan extendPlan = ExecutionPlan.extend(query, nodeIdToSourceSlotMap);

          // Add the slots which are not running
          // Currently not really active since everything is chained
          for (Slot slot : extendPlan.getSlots()) {
              if (!slot.isAlreadyRunning()) {
                  this.executor.submit(slot);
                  this.plan.addSlot(slot);
              }
          }
          runningQueries.add(query);
      }
  }

  public synchronized void deleteQuery(Query query) {
      // Remove nodes from Topology
      currentTopology.removeNodes(query.getTopology().getNodes());
      // Remove operations from slots or slots all
      ArrayList<Slot> deletedSlots = new ArrayList<>();
      for(Slot slot : this.plan.getSlots()) {
          SourceSlot sourceSlot = (SourceSlot) slot;
          if(slot.getAssociatedQueries().contains(query)) {
              if(slot.getAssociatedQueries().size() == 1) {
                  // Delete the slot
                  slot.shutdown();
                  deletedSlots.add(slot);
              }
              else {
                  // Modify the slot
                  sourceSlot.getConnector().removeOperationsAssociatedWith(query);
                  sourceSlot.removeAssociatedQuery(query);
              }
          }
      }
      for(Slot slot : deletedSlots) {
          this.plan.removeSlot(slot);
      }
      this.runningQueries.remove(query);
  }

  private HashMap<UUID, SourceSlot<?>> getSourceNodeToSlotMapping(Topology queryTopology) {
      HashMap<UUID, SourceSlot<?>> nodeIdToSourceSlotMap = new HashMap<>();
      for (Node newSource : queryTopology.getSourceNodes()) {
          if (this.currentTopology.getSourceNodes().contains(newSource)) {
              // We found an identical source, meaning that we can share state
              SourceSlot<?> sourceSlot = (SourceSlot<?>) this.plan.getSlotById(newSource.getNodeId());
              nodeIdToSourceSlotMap.put(newSource.getNodeId(), sourceSlot);
          }
          else {
              this.currentTopology.addNode(newSource);
          }
      }
      log.debug("Identified {} Shared Sources", nodeIdToSourceSlotMap.size());
      return nodeIdToSourceSlotMap;
  }

  public void shutdown() {
    this.executor.shutdownNow();
  }

}
