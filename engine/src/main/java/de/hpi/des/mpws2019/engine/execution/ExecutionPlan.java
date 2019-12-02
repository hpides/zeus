package de.hpi.des.mpws2019.engine.execution;

import de.hpi.des.mpws2019.engine.execution.slot.Slot;
import de.hpi.des.mpws2019.engine.graph.Node;
import de.hpi.des.mpws2019.engine.graph.Topology;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;

public class ExecutionPlan {

  @Getter
  private final List<Slot> slotList;

  private ExecutionPlan(final List<Slot> slotList) {
    this.slotList = slotList;
  }

  public List<Runnable> getSlotListRunnables() {
    return this.slotList.stream().map(Slot::makeRunnable).collect(Collectors.toList());
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
