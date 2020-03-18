package de.hpi.des.hdes.engine.execution.plan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.execution.slot.RunnableSlot;
import de.hpi.des.hdes.engine.execution.slot.Slot;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.Topology;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Data;

@Data
public class ExecutionPlan {

  private final Topology topology;
  private final List<Slot<?>> slots;
  private final Map<Node, Slot<?>> outputSlotMap;

  public ExecutionPlan(final Topology topology, final List<Slot<?>> slots,
      final Map<Node, Slot<?>> outputSlotMap) {
    this.topology = topology;
    this.slots = slots;
    this.outputSlotMap = outputSlotMap;
  }

  private ExecutionPlan() {
    this(Topology.emptyTopology(), Lists.newArrayList(), Maps.newHashMap());
  }

  public List<RunnableSlot<?>> getRunnableSlots() {
    return this.getSlots().stream()
        .filter(slot -> slot instanceof RunnableSlot)
        .map(slot -> (RunnableSlot<?>) slot)
        .collect(Collectors.toList());
  }

  public static ExecutionPlan emptyExecutionPlan() {
    return new ExecutionPlan();
  }

  public static ExecutionPlan extend(final ExecutionPlan executionPlan, final Query query) {
    final Set<Node> topologyNodes = executionPlan.getTopology().getNodes();
    final Topology queryTopology = query.getTopology();
    topologyNodes.forEach(node -> {
      // store associated query in node for later deletion
      // only works this way cause we have to change the actual nodes in the current topology
      if (queryTopology.getNodes().contains(node)) {
        node.addAssociatedQuery(query);
      }
    });
    final Set<Node> newNodes = Sets.difference(queryTopology.getNodes(), topologyNodes);
    newNodes.forEach(node -> node.addAssociatedQuery(query));
    final LocalExecutionPlanBuilder planBuilder = new LocalExecutionPlanBuilder(executionPlan);
    return planBuilder.build(Topology.of(queryTopology.getNodes()));
  }

  public static ExecutionPlan createPlan(final Query query) {
    return extend(emptyExecutionPlan(), query);
  }

  public static ExecutionPlan delete(final ExecutionPlan executionPlan, final Query query) {
    final List<Slot<?>> slots = executionPlan.getSlots();
    final Set<Node> nodes = executionPlan.getTopology().getNodes();

    // this will shutdown a slot when there are no more associated queries
    slots.forEach(slot -> slot.remove(query));
    final List<Slot<?>> runningSlots = slots.stream()
        .filter(slot -> !slot.isShutdown())
        .collect(Collectors.toList());

    // keep only nodes associated with a query
    final Set<Node> currentNodes = nodes.stream()
        .filter(node -> !node.getAssociatedQueries().isEmpty())
        .collect(Collectors.toSet());

    // keep only the outputs of members of the topology
    final Map<Node, Slot<?>> outputSlotMap = Maps.newHashMap(executionPlan.getOutputSlotMap());
    outputSlotMap.keySet().retainAll(currentNodes);

    return new ExecutionPlan(Topology.of(currentNodes), runningSlots, outputSlotMap);
  }
}
