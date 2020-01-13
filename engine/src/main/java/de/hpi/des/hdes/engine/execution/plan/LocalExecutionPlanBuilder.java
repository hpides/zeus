package de.hpi.des.hdes.engine.execution.plan;

import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.execution.connector.Buffer;
import de.hpi.des.hdes.engine.execution.connector.ListConnector;
import de.hpi.des.hdes.engine.execution.slot.Slot;
import de.hpi.des.hdes.engine.execution.slot.SourceSlot;
import de.hpi.des.hdes.engine.execution.slot.TwoInputSlot;
import de.hpi.des.hdes.engine.graph.BinaryOperationNode;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.SinkNode;
import de.hpi.des.hdes.engine.graph.SourceNode;
import de.hpi.des.hdes.engine.graph.UnaryOperationNode;
import de.hpi.des.hdes.engine.operation.OneInputOperator;
import de.hpi.des.hdes.engine.operation.Source;
import de.hpi.des.hdes.engine.operation.TwoInputOperator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalExecutionPlanBuilder implements NodeVisitor {

  private final Map<Node, ListConnector<?>> connectors = new HashMap<>();
  @Getter
  private final List<Slot> slots = new LinkedList<>();

  private final Map<UUID, SourceSlot<?>> sourceSlotMap;
  private final Query query;

  public LocalExecutionPlanBuilder(final Map<UUID, SourceSlot<?>> sourceSlotMap, final Query query) {
    this.sourceSlotMap = sourceSlotMap;
    this.query = query;
  }

  @Override
  public <OUT> void visit(final SourceNode<OUT> sourceNode) {
    final SourceSlot<?> slot;
    if (this.sourceSlotMap.containsKey(sourceNode.getNodeId())) {
      slot = this.sourceSlotMap.get(sourceNode.getNodeId());
      this.connectors.put(sourceNode, slot.getConnector());
      slot.setAlreadyRunning(true);
    } else {
      final Source<OUT> source = sourceNode.getSource();
      final ListConnector<OUT> output = ListConnector.create();
      this.connectors.put(sourceNode, output);
      slot = new SourceSlot<>(source, sourceNode.getNodeId(), output);
    }
    slot.addAssociatedQuery(this.query);
    this.slots.add(slot);
  }

  @Override
  public <IN> void visit(final SinkNode<IN> sinkNode) {
    // Look for cleaner solution with visitor pattern.
    final Node parent = sinkNode.getParents().iterator().next();
    final ListConnector<IN> parentConnector = this.getParentConnector(parent);
    parentConnector.addFunction(sinkNode, sinkNode.getSink());
  }

  @Override
  public <IN, OUT> void visit(final UnaryOperationNode<IN, OUT> unaryOperationNode) {
    final OneInputOperator<IN, OUT> operator = unaryOperationNode.getOperator();

    final ListConnector<OUT> output = ListConnector.create();
    operator.init(output);
    this.connectors.put(unaryOperationNode, output);

    // Look for cleaner solution with visitor pattern.
    final Node parent = unaryOperationNode.getParents().iterator().next();
    final ListConnector<IN> parentConnector = this.getParentConnector(parent);
    parentConnector.addFunction(unaryOperationNode, operator, this.query);
  }


  @Override
  public <IN1, IN2, OUT> void visit(final BinaryOperationNode<IN1, IN2, OUT> binaryOperationNode) {
    final TwoInputOperator<IN1, IN2, OUT> operator = binaryOperationNode.getOperator();

    final ListConnector<OUT> output = ListConnector.create();
    operator.init(output);
    this.connectors.put(binaryOperationNode, output);

    final Iterator<Node> parents = binaryOperationNode.getParents().iterator();
    final ListConnector<IN1> parent1 = this.getParentConnector(parents.next());
    final ListConnector<IN2> parent2 = this.getParentConnector(parents.next());
    final Buffer<IN1> input1 = parent1.addBuffer(binaryOperationNode);
    final Buffer<IN2> input2 = parent2.addBuffer(binaryOperationNode);

    final Slot slot = new TwoInputSlot<IN1, IN2, OUT>(operator, input1, input2, output,
        binaryOperationNode.getNodeId());
    slot.addAssociatedQuery(this.query);
    this.slots.add(slot);
  }

  private <IN> ListConnector<IN> getParentConnector(final Node parent) {
    return (ListConnector<IN>) this.connectors.get(parent);
  }
}
