package de.hpi.des.mpws2019.engine.execution.plan;

import de.hpi.des.mpws2019.engine.execution.connector.QueueBuffer;
import de.hpi.des.mpws2019.engine.execution.connector.QueueConnector;
import de.hpi.des.mpws2019.engine.execution.slot.OneInputSlot;
import de.hpi.des.mpws2019.engine.execution.slot.SinkSlot;
import de.hpi.des.mpws2019.engine.execution.slot.Slot;
import de.hpi.des.mpws2019.engine.execution.slot.SourceSlot;
import de.hpi.des.mpws2019.engine.execution.slot.TwoInputSlot;
import de.hpi.des.mpws2019.engine.graph.BinaryOperationNode;
import de.hpi.des.mpws2019.engine.graph.Node;
import de.hpi.des.mpws2019.engine.graph.NodeVisitor;
import de.hpi.des.mpws2019.engine.graph.SinkNode;
import de.hpi.des.mpws2019.engine.graph.SourceNode;
import de.hpi.des.mpws2019.engine.graph.UnaryOperationNode;
import de.hpi.des.mpws2019.engine.operation.OneInputOperator;
import de.hpi.des.mpws2019.engine.operation.Sink;
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
public class PullExecutionPlanBuilder implements NodeVisitor {

  private final Map<Node, QueueConnector<?>> nodeOutputConnectors = new HashMap<>();
  @Getter
  private final List<Slot> slots = new LinkedList<>();

  @Override
  public void visit(SourceNode sourceNode) {
    Source source = sourceNode.getSource();
    QueueConnector output = new QueueConnector();
    nodeOutputConnectors.put(sourceNode, output);

    Slot slot = new SourceSlot<>(source, output);
    this.slots.add(slot);
  }

  @Override
  public void visit(SinkNode sinkNode) {
    Sink sink = sinkNode.getSink();

    // Look for cleaner solution with visitor pattern.
    final Node parent = sinkNode.getParents().iterator().next();
    final QueueConnector parentConnector = nodeOutputConnectors.get(parent);

    final QueueBuffer parentBuffer = parentConnector.addQueueBuffer(sinkNode);

    Slot slot = new SinkSlot(sink, parentBuffer);
    this.slots.add(slot);
  }

  @Override
  public void visit(UnaryOperationNode unaryOperationNode) {
    QueueConnector output = new QueueConnector();
    OneInputOperator operator = unaryOperationNode.getOperator();
    this.nodeOutputConnectors.put(unaryOperationNode, output);

    // Look for cleaner solution with visitor pattern.
    final Node parent = unaryOperationNode.getParents().iterator().next();
    final QueueConnector parentConnector = nodeOutputConnectors.get(parent);

    final QueueBuffer parentBuffer = parentConnector.addQueueBuffer(unaryOperationNode);

    Slot slot = new OneInputSlot(operator, parentBuffer, output);
    this.slots.add(slot);
  }

  @Override
  public void visit(BinaryOperationNode binaryOperationNode) {
    QueueConnector output = new QueueConnector();
    TwoInputOperator operator = binaryOperationNode.getOperator();
    nodeOutputConnectors.put(binaryOperationNode, output);

    Iterator<Node> parents = binaryOperationNode.getParents().iterator();
    Node parent1 = parents.next();
    Node parent2 = parents.next();

    QueueBuffer parent1Input = nodeOutputConnectors.get(parent1)
        .addQueueBuffer(binaryOperationNode);
    QueueBuffer parent2Input = nodeOutputConnectors.get(parent2)
        .addQueueBuffer(binaryOperationNode);

    Slot slot = new TwoInputSlot(operator, parent1Input, parent2Input, output);
    this.slots.add(slot);
  }
}
