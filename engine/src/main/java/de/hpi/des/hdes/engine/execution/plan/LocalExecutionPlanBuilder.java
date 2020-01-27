package de.hpi.des.hdes.engine.execution.plan;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalExecutionPlanBuilder implements NodeVisitor {

  private final Map<Node, Slot> outputSlots = new HashMap<>();
  private final List<Slot<?>> slots = new LinkedList<>();

  public List<Slot<?>> build(final Query query) {
    final List<Node> sortedNodes = query.getTopology().getTopologicalOrdering();
    for (final Node node : sortedNodes) {
      node.accept(this);
    }
    return this.slots;
  }

  public List<Slot<?>> build(final Topology topology) {
    final List<Node> sortedNodes = topology.getTopologicalOrdering();
    for (final Node node : sortedNodes) {
      node.accept(this);
    }
    return this.slots;
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
    final Slot<IN> parentSlot = this.outputSlots.get(parent);
    parentSlot.addOutput(sinkNode, sinkNode.getSink());
  }

  @Override
  public <IN, OUT> void visit(final UnaryOperationNode<IN, OUT> unaryOperationNode) {
    final OneInputOperator<IN, OUT> operator = unaryOperationNode.getOperator();
    final Node parentNode = unaryOperationNode.getParents().iterator().next();
    final Slot<IN> parentSlot = this.outputSlots.get(parentNode);

    final OneInputPushSlot<IN, OUT> slot = new OneInputPushSlot<>(parentSlot, operator,
        unaryOperationNode);
    parentSlot.addChild(slot);
    parentSlot.addOutput(unaryOperationNode, operator);
    operator.init(slot);

    this.outputSlots.put(unaryOperationNode, slot);
    this.slots.add(slot);
  }

  @Override
  public <IN1, IN2, OUT> void visit(final BinaryOperationNode<IN1, IN2, OUT> binaryOperationNode) {
    final TwoInputOperator<IN1, IN2, OUT> operator = binaryOperationNode.getOperator();
    final Iterator<Node> parents = binaryOperationNode.getParents().iterator();
    final Node parentNode1 = parents.next();
    final Node parentNode2 = parents.next();
    final Slot<IN1> parent1Slot = this.outputSlots.get(parentNode1);
    final Slot<IN2> parent2Slot = this.outputSlots.get(parentNode2);
    final Buffer<IN1> input1 = Buffer.create();
    final Buffer<IN2> input2 = Buffer.create();
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
}
