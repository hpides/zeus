package de.hpi.des.hdes.engine.execution.plan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.execution.slot.Slot;
import de.hpi.des.hdes.engine.execution.slot.VulcanoRunnableSlot;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.Topology;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Data;

/**
 * The execution plan describes the current way the engine operates.
 *
 * The plan is mainly based on two parts: a list of slots {@link Slot} and a
 * mapping. The list of slots is used to execute all slots accordingly. The
 * mapping allows for interaction between the {@link Topology} and the execution
 * plan. For example, it allows the deletion of a node.
 */
@Data
public class VulcanoExecutionPlan {

  private final Topology topology;
  private final List<Slot<?>> slots;
  private final Map<Node, Slot<?>> outputSlotMap;

  public VulcanoExecutionPlan(final Topology topology, final List<Slot<?>> slots,
      final Map<Node, Slot<?>> outputSlotMap) {
    this.topology = topology;
    this.slots = slots;
    this.outputSlotMap = outputSlotMap;
  }

  private VulcanoExecutionPlan() {
    this(Topology.emptyTopology(), Lists.newArrayList(), Maps.newHashMap());
  }

  /**
   * @return a list of runnable slots
   */
  public List<VulcanoRunnableSlot<?>> getRunnableSlots() {
    return this.getSlots().stream().filter(slot -> slot instanceof VulcanoRunnableSlot)
        .map(slot -> (VulcanoRunnableSlot<?>) slot).collect(Collectors.toList());
  }

  public static VulcanoExecutionPlan emptyExecutionPlan() {
    return new VulcanoExecutionPlan();
  }

  /**
   * Extends an execution plan with a new query.
   *
   * @param executionPlan the plan to extend
   * @param query         the new query
   * @return a new plan
   */
  public static VulcanoExecutionPlan extend(final VulcanoExecutionPlan executionPlan, final Query query) {
    final Set<Node> topologyNodes = executionPlan.getTopology().getNodes();
    final Topology queryTopology = query.getTopology();
    topologyNodes.forEach(node -> {
      // store associated query in node for later deletion
      // only works this way cause we have to change the actual nodes in the current
      // topology
      if (queryTopology.getNodes().contains(node)) {
        node.addAssociatedQuery(query);
      }
    });
    final Set<Node> newNodes = Sets.difference(queryTopology.getNodes(), topologyNodes);
    newNodes.forEach(node -> node.addAssociatedQuery(query));
    final LocalExecutionPlanBuilder planBuilder = new LocalExecutionPlanBuilder(executionPlan);
    return planBuilder.build(Topology.of(queryTopology.getNodes()));
  }

  /**
   * Creates a execution plan with a single query.
   *
   * @param query the query to use
   * @return a new execution plan
   */
  public static VulcanoExecutionPlan createPlan(final Query query) {
    return extend(emptyExecutionPlan(), query);
  }

  /**
   * Deletes a query from an execution plan
   *
   * @param executionPlan the execution plan to delete the query from
   * @param query         the query to delete
   * @return a new execution plan
   */
  public static VulcanoExecutionPlan delete(final VulcanoExecutionPlan executionPlan, final Query query) {
    final List<Slot<?>> slots = executionPlan.getSlots();
    final Set<Node> nodes = executionPlan.getTopology().getNodes();

    // this will shutdown a slot when there are no more associated queries
    slots.forEach(slot -> slot.remove(query));
    final List<Slot<?>> runningSlots = slots.stream().filter(slot -> !slot.isShutdown()).collect(Collectors.toList());

    // keep only nodes associated with a query
    final Set<Node> currentNodes = nodes.stream().filter(node -> !node.getAssociatedQueries().isEmpty())
        .collect(Collectors.toSet());

    // keep only the outputs of members of the topology
    final Map<Node, Slot<?>> outputSlotMap = Maps.newHashMap(executionPlan.getOutputSlotMap());
    outputSlotMap.keySet().retainAll(currentNodes);

    return new VulcanoExecutionPlan(Topology.of(currentNodes), runningSlots, outputSlotMap);
  }
}
