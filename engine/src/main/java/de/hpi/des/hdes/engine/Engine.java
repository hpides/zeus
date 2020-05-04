package de.hpi.des.hdes.engine;

import de.hpi.des.hdes.engine.execution.plan.ExecutionPlan;
import de.hpi.des.hdes.engine.execution.slot.RunnableSlot;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * The engine is the main service of HDES.
 *
 * It is either running or not running. In both states it is possible to add or remove queries.
 */
@Slf4j
public class Engine {

  private final List<Query> runningQueries = new ArrayList<>();

  private ExecutionPlan plan;
  private final ExecutorService executor;
  @Getter
  private boolean isRunning;

  public Engine() {
    this.plan = ExecutionPlan.emptyExecutionPlan();
    this.executor = Executors.newCachedThreadPool();
    this.isRunning = false;
  }

  /**
   * Run the engine
   */
  public void run() {
    log.info("Starting Engine");
    if (this.isRunning) {
      throw new IllegalStateException("Engine already running");
    }

    this.isRunning = true;
    for (final RunnableSlot<?> slot : this.plan.getRunnableSlots()) {
      log.debug("Slot {} submitted", slot);
      slot.setEnabled(true);
      this.executor.submit(slot);
    }
  }

  /**
   * Add a new query to the execution
   *
   * Note: We synchronize this method to avoid problems when multiple queries are added or
   * deleted at the same time
   *
   * @param query the new query
   */
  public synchronized void addQuery(final Query query) {
    final ExecutionPlan extendedPlan = ExecutionPlan.extend(this.plan, query);
    if (this.isRunning) {
      extendedPlan.getRunnableSlots()
          .stream()
          .filter(slot -> !slot.isEnabled())
          .forEach(slot -> {
            log.debug("Submitted slot {}", slot);
            slot.setEnabled(true);
            this.executor.submit(slot);
          });
    }
    this.plan = extendedPlan;
    log.info("Attaches Query {}", query.getId());
    this.runningQueries.add(query);
  }

  /**
   * Deletes a query from the execution plan
   *
   * Note: We synchronize this method to avoid problems when multiple queries are added or
   * deleted at the same time
   *
   * @param query the query to delete
   */
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
    // TODO: Do Sinks also require a shutdown function (e.g. flush a FileSink)? Possible solution: getSlotProcessor() function in exectution plan and add a shutdown function to the SlotProcessor interface

    this.executor.shutdownNow();
  }

  public ExecutionPlan getPlan() {
    return this.plan;
  }
}
