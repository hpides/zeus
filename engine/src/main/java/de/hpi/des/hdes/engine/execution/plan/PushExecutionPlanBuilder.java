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
public class PushExecutionPlanBuilder implements NodeVisitor {

  private final Map<Node, ListConnector<?>> connectors = new HashMap<>();
  @Getter
  private final List<Slot> slots = new LinkedList<>();

  private final Map<UUID, SourceSlot<?>> sourceSlotMap;
  private final Query query;

  public PushExecutionPlanBuilder(Map<UUID, SourceSlot<?>> sourceSlotMap, Query query) {
    this.sourceSlotMap = sourceSlotMap;
    this.query = query;
  }

  @Override
  public <OUT> void visit(SourceNode<OUT> sourceNode) {
    SourceSlot<?> slot;
    if(sourceSlotMap.containsKey(sourceNode.getNodeId())) {
      slot = sourceSlotMap.get(sourceNode.getNodeId());
      connectors.put(sourceNode, slot.getConnector());
      slot.setAlreadyRunning(true);
    }
    else {
      Source<OUT> source = sourceNode.getSource();
      ListConnector<OUT> output = ListConnector.create();
      connectors.put(sourceNode, output);
      slot = new SourceSlot<>(source, sourceNode.getNodeId(), output);
    }
    slot.addAssociatedQuery(query);
    this.slots.add(slot);
  }

  @Override
  public <IN> void visit(SinkNode<IN> sinkNode) {
    // Look for cleaner solution with visitor pattern.
    final Node parent = sinkNode.getParents().iterator().next();
    final ListConnector<IN> parentConnector = (ListConnector<IN>) connectors.get(parent);
    parentConnector.addFunction(sinkNode, sinkNode.getSink());
  }

  @Override
  public <IN,OUT> void visit(UnaryOperationNode<IN,OUT> unaryOperationNode) {
    OneInputOperator<IN,OUT> operator = unaryOperationNode.getOperator();

    ListConnector<OUT> output = ListConnector.create();
    operator.init(output);
    connectors.put(unaryOperationNode, output);

    // Look for cleaner solution with visitor pattern.
    final Node parent = unaryOperationNode.getParents().iterator().next();
    final ListConnector<IN>  parentConnector = (ListConnector<IN>) connectors.get(parent);
    parentConnector.addFunction(unaryOperationNode, operator, query);
  }

  @Override
  public <IN1,IN2,OUT> void visit(BinaryOperationNode<IN1,IN2,OUT> binaryOperationNode) {
    TwoInputOperator<IN1,IN2,OUT> operator = binaryOperationNode.getOperator();

    ListConnector<OUT> output = ListConnector.create();
    operator.init(output);
    connectors.put(binaryOperationNode, output);

    Iterator<Node> parents = binaryOperationNode.getParents().iterator();
    Node parent1 = parents.next();
    Node parent2 = parents.next();
    Buffer<IN1> input1 = (Buffer<IN1>) connectors.get(parent1).addBuffer(binaryOperationNode);
    Buffer<IN2> input2 = (Buffer<IN2>) connectors.get(parent2).addBuffer(binaryOperationNode);

    Slot slot = new TwoInputSlot<IN1,IN2,OUT>(operator, input1, input2, output, binaryOperationNode.getNodeId());
    slot.addAssociatedQuery(query);
    this.slots.add(slot);
  }
}
