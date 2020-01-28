package de.hpi.des.hdes.engine.execution.plan;

import com.google.common.collect.Sets;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.execution.slot.RunnableSlot;
import de.hpi.des.hdes.engine.execution.slot.Slot;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.Topology;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ExecutionPlan {

  private final LocalExecutionPlanBuilder builder;
  private final Topology topology;
  private final List<Slot<?>> slots;

  private ExecutionPlan(final Topology topology, final List<Slot<?>> slots,
      final LocalExecutionPlanBuilder builder) {
    this.topology = topology;
    this.slots = slots;
    this.builder = builder;
  }

  public static ExecutionPlan emptyExecutionPlan() {
    return new ExecutionPlan(new Topology(Set.of()), List.of(), new LocalExecutionPlanBuilder());
  }

  public ExecutionPlan extend(final Query query) {
    final Set<Node> topologyNodes = this.topology.getNodes();
    final Topology queryTopology = query.getTopology();
    topologyNodes.forEach(node -> {
      // store associated query in node for later deletion
      // only works this way cause we have to change the actual nodes in the current topology
      if (queryTopology.getNodes().contains(node)) {
        node.addAssociatedQuery(query);
      }
    });
    final Set<Node> toBeSubmitted = Sets.difference(queryTopology.getNodes(), topologyNodes);
    toBeSubmitted.forEach(node -> node.addAssociatedQuery(query));
    final List<Slot<?>> newSlots = List.copyOf(this.builder.build(new Topology(toBeSubmitted)));
    final Topology newTopology = this.topology.extend(queryTopology);
    return new ExecutionPlan(newTopology, newSlots, this.builder);
  }

  public ExecutionPlan delete(final Query query) {
    this.slots.forEach(slot -> slot.remove(query));
    final List<Slot<?>> runningSlots = this.slots.stream().filter(slot -> !slot.isShutdown())
        .collect(Collectors.toList());

    this.topology.getNodes().forEach(node -> {
      node.removeAssociatedQuery(query);
    });

    final Set<Node> currentNodes = this.topology.getNodes().stream()
        .filter(node -> !node.getAssociatedQueries().isEmpty())
        .collect(Collectors.toSet());

    return new ExecutionPlan(new Topology(currentNodes), runningSlots, this.builder);
  }

  public List<RunnableSlot<?>> getRunnableSlots() {
    return this.getSlots().stream()
        .filter(slot -> slot instanceof RunnableSlot)
        .map(slot -> (RunnableSlot<?>) slot)
        .collect(Collectors.toList());
  }

  public Topology getTopology() {
    return this.topology;
  }

  public List<Slot<?>> getSlots() {
    return this.slots;
  }
}
