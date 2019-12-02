package de.hpi.des.mpws2019.engine.execution.plan;

import de.hpi.des.mpws2019.engine.execution.slot.Slot;
import de.hpi.des.mpws2019.engine.graph.Node;
import de.hpi.des.mpws2019.engine.graph.Topology;
import java.util.List;

public class ExecutionPlan {

  private final List<Slot> slots;

  private ExecutionPlan(final List<Slot> slots) {
    this.slots = slots;
  }

  public List<Slot> getSlots() {
    return this.slots;
  }

  public static ExecutionPlan from(final Topology topology) {
    final List<Node> sortedNodes = topology.getTopologicalOrdering();
    final PushExecutionPlanBuilder visitor = new PushExecutionPlanBuilder();

    for (Node node: sortedNodes) {
      node.accept(visitor);
    }

    final List<Slot> slots = visitor.getSlots();
    return new ExecutionPlan(slots);
  }
}
