package de.hpi.des.hdes.engine.execution.plan;

import de.hpi.des.hdes.engine.execution.connector.Buffer;
import de.hpi.des.hdes.engine.execution.connector.Connector;
import de.hpi.des.hdes.engine.execution.slot.TwoInputSlot;
import de.hpi.des.hdes.engine.operation.TwoInputOperator;
import de.hpi.des.hdes.engine.execution.slot.Slot;
import de.hpi.des.hdes.engine.execution.slot.SourceSlot;
import de.hpi.des.hdes.engine.graph.BinaryOperationNode;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.SinkNode;
import de.hpi.des.hdes.engine.graph.SourceNode;
import de.hpi.des.hdes.engine.graph.UnaryOperationNode;
import de.hpi.des.hdes.engine.operation.OneInputOperator;
import de.hpi.des.hdes.engine.operation.Source;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PushExecutionPlanBuilder implements NodeVisitor {

  private final Map<Node, Connector> connectors = new HashMap<>();
  @Getter
  private final List<Slot> slots = new LinkedList<>();

  @Override
  public void visit(SourceNode sourceNode) {
    Source source = sourceNode.getSource();
    Connector output = new Connector();
    connectors.put(sourceNode, output);

    Slot slot = new SourceSlot<>(source, output);
    this.slots.add(slot);
  }

  @Override
  public void visit(SinkNode sinkNode) {
    // Look for cleaner solution with visitor pattern.
    final Node parent = sinkNode.getParents().iterator().next();
    final Connector parentConnector = connectors.get(parent);
    parentConnector.addFunction(sinkNode, sinkNode.getSink()::process);
  }

  @Override
  public void visit(UnaryOperationNode unaryOperationNode) {
    OneInputOperator operator = unaryOperationNode.getOperator();

    Connector output = new Connector();
    operator.init(output);
    connectors.put(unaryOperationNode, output);

    // Look for cleaner solution with visitor pattern.
    final Node parent = unaryOperationNode.getParents().iterator().next();
    final Connector parentConnector = connectors.get(parent);
    parentConnector.addFunction(unaryOperationNode, operator::process);
  }

  @Override
  public void visit(BinaryOperationNode binaryOperationNode) {
    TwoInputOperator operator = binaryOperationNode.getOperator();

    Connector output = new Connector();
    operator.init(output);
    connectors.put(binaryOperationNode, output);

    Iterator<Node> parents = binaryOperationNode.getParents().iterator();
    Node parent1 = parents.next();
    Node parent2 = parents.next();
    Buffer input1 = connectors.get(parent1).addBuffer(binaryOperationNode);
    Buffer input2 = connectors.get(parent2).addBuffer(binaryOperationNode);

    Slot slot = new TwoInputSlot<>(operator, input1, input2, output);
    this.slots.add(slot);
  }
}
