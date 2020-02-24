package de.hpi.des.hdes.engine.execution.plan;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.execution.connector.Buffer;
import de.hpi.des.hdes.engine.execution.slot.OneInputPushSlot;
import de.hpi.des.hdes.engine.execution.slot.Slot;
import de.hpi.des.hdes.engine.execution.slot.SourceSlot;
import de.hpi.des.hdes.engine.execution.slot.TwoInputPullSlot;
import de.hpi.des.hdes.engine.graph.BinaryOperationNode;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.SinkNode;
import de.hpi.des.hdes.engine.graph.SourceNode;
import de.hpi.des.hdes.engine.graph.Topology;
import de.hpi.des.hdes.engine.graph.UnaryOperationNode;
import de.hpi.des.hdes.engine.operation.OneInputOperator;
import de.hpi.des.hdes.engine.operation.Source;
import de.hpi.des.hdes.engine.operation.TwoInputOperator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalExecutionPlanBuilder implements NodeVisitor {

  private final Topology topology;
  private final List<Slot<?>> slots;
  private final Map<Node, Slot<?>> outputSlots;

  public LocalExecutionPlanBuilder(final Topology topology, final List<Slot<?>> slots,
      final Map<Node, Slot<?>> outputSlots) {
    this.topology = topology;
    this.slots = slots;
    this.outputSlots = outputSlots;
  }

  public LocalExecutionPlanBuilder(final ExecutionPlan oldPlan) {
    this(oldPlan.getTopology(), oldPlan.getSlots(), oldPlan.getOutputSlotMap());
  }

  public ExecutionPlan build(final Topology queryTopology) {
    final List<Node> sortedNodes = queryTopology.getTopologicalOrdering();
    for (final Node node : sortedNodes) {
      node.accept(this);
    }
    final Topology updated = this.topology.extend(queryTopology);
    return new ExecutionPlan(updated, this.slots, this.outputSlots);
  }

  public ExecutionPlan build(final Query query) {
    return this.build(query.getTopology());
  }

  @Override
  public <OUT> void visit(final SourceNode<OUT> sourceNode) {
    final Source<OUT> source = sourceNode.getSource();
    final SourceSlot<OUT> slot = new SourceSlot<>(source, sourceNode);
    source.init(slot);
    this.outputSlots.put(sourceNode, slot);
    this.slots.add(slot);
  }

  @Override
  public <IN> void visit(final SinkNode<IN> sinkNode) {
    final Node parent = sinkNode.getParents().iterator().next();
    final Slot<IN> parentSlot = this.getParentSlot(parent);
    parentSlot.addOutput(sinkNode, sinkNode.getSink());
  }

  @Override
  public <IN, OUT> void visit(final UnaryOperationNode<IN, OUT> unaryOperationNode) {
    final OneInputOperator<IN, OUT> operator = unaryOperationNode.getOperator();
    final Node parentNode = unaryOperationNode.getParents().iterator().next();
    final Slot<IN> parentSlot = this.getParentSlot(parentNode);

    final OneInputPushSlot<IN, OUT> slot = new OneInputPushSlot<>(parentSlot, operator,
        unaryOperationNode);

    operator.init(slot);

    this.outputSlots.put(unaryOperationNode, slot);
    this.slots.add(slot);

    parentSlot.addChild(slot);
    parentSlot.addOutput(unaryOperationNode, operator);
  }

  @Override
  public <IN1, IN2, OUT> void visit(final BinaryOperationNode<IN1, IN2, OUT> binaryOperationNode) {
    final TwoInputOperator<IN1, IN2, OUT> operator = binaryOperationNode.getOperator();
    final Iterator<Node> parents = binaryOperationNode.getParents().iterator();
    final Node parentNode1 = parents.next();
    final Node parentNode2 = parents.next();
    final Slot<IN1> parent1Slot = this.getParentSlot(parentNode1);
    final Slot<IN2> parent2Slot = this.getParentSlot(parentNode2);
    final Buffer<AData<IN1>> input1 = Buffer.createADataBuffer();
    final Buffer<AData<IN2>> input2 = Buffer.createADataBuffer();
    parent1Slot.addOutput(binaryOperationNode, input1);
    parent2Slot.addOutput(binaryOperationNode, input2);

    final TwoInputPullSlot<IN1, IN2, OUT> slot = new TwoInputPullSlot<>(operator, parent1Slot,
        parent2Slot, input1, input2, binaryOperationNode);
    parent1Slot.addChild(slot);
    parent2Slot.addChild(slot);
    operator.init(slot);

    this.outputSlots.put(binaryOperationNode, slot);
    this.slots.add(slot);
  }

  @SuppressWarnings("unchecked")
  private <IN> Slot<IN> getParentSlot(final Node parent) {
    return (Slot<IN>) this.outputSlots.get(parent);
  }
}
