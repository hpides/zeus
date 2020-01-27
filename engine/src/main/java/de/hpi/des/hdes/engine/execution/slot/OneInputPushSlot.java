package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.graph.UnaryOperationNode;
import de.hpi.des.hdes.engine.operation.OneInputOperator;
import lombok.Getter;

@Getter
public class OneInputPushSlot<IN, OUT> extends Slot<OUT> {

  private final Slot<IN> parent;
  private final OneInputOperator<IN, OUT> operator;
  private final UnaryOperationNode<IN, OUT> topologyNode;

  public OneInputPushSlot(final Slot<IN> parent, final OneInputOperator<IN, OUT> operator,
      final UnaryOperationNode<IN, OUT> topologyNode) {
    this.operator = operator;
    this.parent = parent;
    this.topologyNode = topologyNode;
  }

  @Override
  public UnaryOperationNode<IN, OUT> getTopologyNode() {
    return this.topologyNode;
  }
}
