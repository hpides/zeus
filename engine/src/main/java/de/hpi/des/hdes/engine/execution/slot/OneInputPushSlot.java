package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.graph.UnaryOperationNode;
import de.hpi.des.hdes.engine.operation.OneInputOperator;
import lombok.Getter;

/**
 * Wraps a {@link OneInputOperator} which transforms stream events from IN to OUT.
 * @param <IN> input elements
 * @param <OUT> output elements
 */
@Getter
public class OneInputPushSlot<IN, OUT> extends Slot<OUT> {

  private final Slot<IN> parent;
  private final OneInputOperator<IN, OUT> operator;
  private final UnaryOperationNode<IN, OUT> topologyNode;

  /**
   *
   * @param parent parent slot which writes into the wrapped operator
   * @param operator wrapped operator which output is routed via thia slot
   * @param topologyNode node of the logical plan which is represented by this slot in the execution plan
   */
  public OneInputPushSlot(final Slot<IN> parent, final OneInputOperator<IN, OUT> operator,
      final UnaryOperationNode<IN, OUT> topologyNode) {
    this.operator = operator;
    this.parent = parent;
    this.topologyNode = topologyNode;
  }

  /**
   * Returns the associated topology node.
   * @return associated topology node of the logical plan.
   */
  @Override
  public UnaryOperationNode<IN, OUT> getTopologyNode() {
    return this.topologyNode;
  }
}
