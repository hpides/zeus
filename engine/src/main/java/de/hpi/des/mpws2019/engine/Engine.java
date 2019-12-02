package de.hpi.des.mpws2019.engine;

import de.hpi.des.mpws2019.engine.execution.plan.ExecutionPlan;
import de.hpi.des.mpws2019.engine.graph.Topology;
import de.hpi.des.mpws2019.engine.graph.TopologyBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    for (var c : this.plan.getSlots()) {
      this.executor.submit(c);
    }
  }

  public void shutdown() {
    this.executor.shutdownNow();
  }

}
