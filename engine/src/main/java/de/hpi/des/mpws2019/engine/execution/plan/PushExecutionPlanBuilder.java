package de.hpi.des.mpws2019.engine.execution.plan;

import de.hpi.des.mpws2019.engine.execution.connector.PushConnector;
import de.hpi.des.mpws2019.engine.execution.slot.Slot;
import de.hpi.des.mpws2019.engine.execution.slot.SourceSlot;
import de.hpi.des.mpws2019.engine.graph.BinaryOperationNode;
import de.hpi.des.mpws2019.engine.graph.Node;
import de.hpi.des.mpws2019.engine.graph.NodeVisitor;
import de.hpi.des.mpws2019.engine.graph.SinkNode;
import de.hpi.des.mpws2019.engine.graph.SourceNode;
import de.hpi.des.mpws2019.engine.graph.UnaryOperationNode;
import de.hpi.des.mpws2019.engine.operation.OneInputOperator;
import de.hpi.des.mpws2019.engine.operation.Source;
import de.hpi.des.mpws2019.engine.operation.TwoInputOperator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PushExecutionPlanBuilder implements NodeVisitor {

  private final Map<Node, PushConnector> nodeOutputConnectors = new HashMap<>();
  @Getter
  private final List<Slot> slots = new LinkedList<>();

  @Override
  public void visit(SourceNode sourceNode) {
    Source source = sourceNode.getSource();
    PushConnector output = new PushConnector();
    nodeOutputConnectors.put(sourceNode, output);

    Slot slot = new SourceSlot<>(source, output);
    this.slots.add(slot);
  }

  @Override
  public void visit(SinkNode sinkNode) {
    // Look for cleaner solution with visitor pattern.
    final Node parent = sinkNode.getParents().iterator().next();
    final PushConnector parentConnector = nodeOutputConnectors.get(parent);
    parentConnector.addFunction(sinkNode, sinkNode.getSink()::process);
  }

  @Override
  public void visit(UnaryOperationNode unaryOperationNode) {
    OneInputOperator operator = unaryOperationNode.getOperator();

    PushConnector output = new PushConnector();
    operator.init(output);
    nodeOutputConnectors.put(unaryOperationNode, output);

    // Look for cleaner solution with visitor pattern.
    final Node parent = unaryOperationNode.getParents().iterator().next();
    final PushConnector parentConnector = nodeOutputConnectors.get(parent);
    parentConnector.addFunction(unaryOperationNode, operator::process);
  }

  @Override
  public void visit(BinaryOperationNode binaryOperationNode) {
    TwoInputOperator operator = binaryOperationNode.getOperator();

    PushConnector output = new PushConnector();
    operator.init(output);
    nodeOutputConnectors.put(binaryOperationNode, output);

    Iterator<Node> parents = binaryOperationNode.getParents().iterator();
    Node parent1 = parents.next();
    Node parent2 = parents.next();
    nodeOutputConnectors.get(parent1).addFunction(binaryOperationNode, operator::processStream1);
    nodeOutputConnectors.get(parent2).addFunction(binaryOperationNode, operator::processStream2);
  }
}
