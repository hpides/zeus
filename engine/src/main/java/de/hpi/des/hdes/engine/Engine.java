package de.hpi.des.hdes.engine;

import de.hpi.des.hdes.engine.execution.plan.ExecutionPlan;
import de.hpi.des.hdes.engine.execution.slot.RunnableSlot;
import de.hpi.des.hdes.engine.shared.join.node.AJoinNode;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Engine {

  private ExecutionPlan plan;
  private final ExecutorService executor;
  private boolean isRunning;

  public Engine() {
    this.plan = ExecutionPlan.emptyExecutionPlan();
    this.executor = Executors.newCachedThreadPool();
    this.isRunning = false;
  }

  public void run() {
    this.isRunning = true;
    for (final RunnableSlot<?> slot : this.plan.getRunnableSlots()) {
      this.executor.submit(slot);
    }
  }

  public synchronized void addQuery(final Query query) {
    final ExecutionPlan extendedPlan = this.plan.extend(query);
    // find AJoins that are already part of the topology and append output from new query
    for (final AJoinNode<?, ?, ?> aJoinNode : query.getTopology().getAJoinNodes()) {
      for (final RunnableSlot runnableSlot : this.plan.getRunnableSlots()) {
        if (runnableSlot.getTopologyNode().equals(aJoinNode)) {
          runnableSlot.addOutput(aJoinNode, aJoinNode.getSink());
        }
      }
    }
    if (this.isRunning) {
      extendedPlan.getRunnableSlots()
          .stream()
          .filter(slot -> !slot.isRunning())
          .forEach(this.executor::submit);
    }
    this.plan = extendedPlan;
  }

  public void deleteQuery(final Query query) {
    if (!this.isRunning || this.plan.getTopology().getNodes().isEmpty()) {
      throw new UnsupportedOperationException("There are no queries");
    }
    this.plan.getSlots().forEach(slot -> slot.remove(query));
  }

  public void shutdown() {
    this.isRunning = false;
    this.executor.shutdownNow();
  }

  public ExecutionPlan getPlan() {
    return this.plan;
  }
}
