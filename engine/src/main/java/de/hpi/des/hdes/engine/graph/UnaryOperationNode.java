package de.hpi.des.hdes.engine.graph;

import de.hpi.des.hdes.engine.operation.OneInputOperator;

public class UnaryOperationNode<IN, OUT> extends Node {

  @Override
  public void accept(NodeVisitor visitor) {
    visitor.visit(this);
  }

  private final OneInputOperator<IN, OUT> operator;

  public UnaryOperationNode(final OneInputOperator<IN, OUT> operator) {
    this.operator = operator;
  }

  public OneInputOperator<IN, OUT> getOperator() {
    return this.operator;
  }
}
