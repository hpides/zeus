package de.hpi.des.mpws2019.engine.execution;

import de.hpi.des.mpws2019.engine.execution.slot.InputBuffer;
import de.hpi.des.mpws2019.engine.execution.slot.OneInputSlot;
import de.hpi.des.mpws2019.engine.execution.slot.QueueBuffer;
import de.hpi.des.mpws2019.engine.execution.slot.QueueConnector;
import de.hpi.des.mpws2019.engine.execution.slot.Slot;
import de.hpi.des.mpws2019.engine.execution.slot.SourceSlot;
import de.hpi.des.mpws2019.engine.execution.slot.TwoInputSlot;
import de.hpi.des.mpws2019.engine.graph.BinaryOperationNode;
import de.hpi.des.mpws2019.engine.graph.Node;
import de.hpi.des.mpws2019.engine.graph.SourceNode;
import de.hpi.des.mpws2019.engine.graph.Topology;
import de.hpi.des.mpws2019.engine.graph.UnaryOperationNode;
import de.hpi.des.mpws2019.engine.operation.BinaryOperator;
import de.hpi.des.mpws2019.engine.operation.OneInputOperator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;

public class ExecutionPlan {

  @Getter
  private final List<Slot> slotList;

  private ExecutionPlan(final List<Slot> slotList) {
    this.slotList = slotList;
  }

  public List<Runnable> getSlotListRunnables() {
    return this.slotList.stream().map(Slot::makeRunnable).collect(Collectors.toList());
  }

  public static ExecutionPlan from(final Topology topology) {
    final List<Slot> slotList = new ArrayList<>();
    final HashMap<BinaryOperationNode<?, ?, ?>, InputBuffer<?>> unfinishedNodes = new HashMap<>();

    for (final SourceNode<?> sourceNode : topology.getSourceNodes()) {
      final QueueConnector<?> queueConnector = new QueueConnector();
      final QueueBuffer<?> queueBuffer = queueConnector.addQueueBuffer(sourceNode.getNodeId());

      slotList.add(new SourceSlot(sourceNode.getSource(), queueConnector));
      for (final Node node : sourceNode.getChildren()) {
        traverseUntilBlock(node, queueBuffer, unfinishedNodes, slotList);
      }
    }
    return new ExecutionPlan(slotList);
  }

  /**
   * Traverses a DAG depth first until a blocking node is encountered.
   *
   * Consider the following DAG:
   *
   *  SourceNode    SourceNode
   *    |               |
   *    |               |
   *    v               |
   *  UnaryNode         |
   *    |               |
   *    |               |
   *    -->  JoinNode <--
   *            |
   *            |
   *            v
   *          Sink
   *
   *
   * The method starts with the left SourceNode and traverses the DAG depth first. For each node,
   * it creates a new corresponding Slot. However, it can not instantiate the JoinNode the first
   * time visiting it as this requires the other Node's input. Therefore, it is saved in a map with
   * the input from the current parent as value and abort the current traverse.
   * As the second SourceNode is traversed, the JoinNode can be instantiated because both inputs are
   * known.
   *
   * TODO: Probably better to implement a Visitor pattern on the nodes
   *
   * @param node current Node
   * @param inputFromParent the input created by the parent node
   * @param unfinishedNodes current nodes that miss a second input
   * @param slotList all slots
   */
  private static void traverseUntilBlock(final Node node, final InputBuffer<?> inputFromParent,
                                         final Map<BinaryOperationNode<?, ?, ?>, InputBuffer<?>> unfinishedNodes,
                                         final List<Slot> slotList) {
    if (node instanceof BinaryOperationNode && !unfinishedNodes.containsKey(node)) {
      final BinaryOperationNode<?, ?, ?> node1 = (BinaryOperationNode<?, ?, ?>) node;
      unfinishedNodes.put(node1, inputFromParent);
      return;
    }

    final QueueConnector collector = new QueueConnector();

    Collection<Node> children = Collections.emptyList();

    if (node instanceof UnaryOperationNode) {
      final OneInputOperator operator = ((UnaryOperationNode) node).getOperator();

      slotList.add(new OneInputSlot(operator, inputFromParent, collector));
      children = node.getChildren();
    } else if (node instanceof BinaryOperationNode) {
      final BinaryOperator operator = ((BinaryOperationNode) node).getOperator();
      slotList.add(
          new TwoInputSlot(operator, unfinishedNodes.get(node), inputFromParent,
              collector));
      unfinishedNodes.remove(operator);
      children = node.getChildren();
    }

    final QueueBuffer objectOutputBuffer = collector.addQueueBuffer(node.getNodeId());
    for (final Node node1 : children) {
      traverseUntilBlock(node1, objectOutputBuffer, unfinishedNodes, slotList);
    }
  }

}
