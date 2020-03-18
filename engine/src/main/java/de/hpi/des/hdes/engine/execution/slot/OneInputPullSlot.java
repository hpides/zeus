package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.execution.connector.Buffer;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.UnaryOperationNode;

public class OneInputPullSlot<IN, OUT> extends RunnableSlot<OUT> {

  private final UnaryOperationNode<IN, OUT> node;
  private final Buffer<AData<IN>> input;

  public OneInputPullSlot(final UnaryOperationNode<IN, OUT> node, final Buffer<AData<IN>> input) {
    this.node = node;
    this.input = input;
  }

  @Override
  public void runStep() {
    final AData<IN> in = this.input.poll();
    if (in != null) {
      this.node.getOperator().process(in);
    }
  }

  @Override
  public Node getTopologyNode() {
    return this.node;
  }
}
