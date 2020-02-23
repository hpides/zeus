package de.hpi.des.hdes.engine;

import de.hpi.des.hdes.engine.execution.plan.ExecutionPlan;
import de.hpi.des.hdes.engine.execution.slot.RunnableSlot;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Engine {

  private final List<Query> runningQueries = new ArrayList<>();

  private ExecutionPlan plan;
  private final ExecutorService executor;
  private boolean isRunning;

  public Engine() {
    this.plan = ExecutionPlan.emptyExecutionPlan();
    this.executor = Executors.newCachedThreadPool();
    this.isRunning = false;
  }

  public void run() {
    if (this.isRunning) {
      throw new IllegalStateException("Engine already running");
    }

    this.isRunning = true;
    for (final RunnableSlot<?> slot : this.plan.getRunnableSlots()) {
      log.debug("Slot {} submitted", slot);
      this.executor.submit(slot);
    }
  }

  public synchronized void addQuery(final Query query) {
    // We synchronize this method to avoid problems when multiple queries are added or
    // deleted at the same time
    final ExecutionPlan extendedPlan = ExecutionPlan.extend(this.plan, query);;
    if (this.isRunning) {
      extendedPlan.getRunnableSlots()
          .stream()
          .filter(slot -> !slot.isRunning())
          .forEach(task -> {
            log.debug("Submitted slot {}", task);
            this.executor.submit(task);
          });
    }
    this.plan = extendedPlan;
    log.info("Attaches Query {}", query.getId());
    this.runningQueries.add(query);
  }

  public synchronized void deleteQuery(final Query query) {
    // We synchronize this method to avoid problems when multiple queries are added or
    // deleted at the same time
    if (this.plan.getTopology().getNodes().isEmpty()) {
      throw new UnsupportedOperationException("There are no queries");
    }
    this.plan = ExecutionPlan.delete(this.plan, query);
    this.runningQueries.remove(query);
  }

  public void shutdown() {
    this.isRunning = false;
    this.plan.getRunnableSlots().forEach(RunnableSlot::shutdown);
    this.executor.shutdownNow();
  }

  public ExecutionPlan getPlan() {
    return this.plan;
  }
}
