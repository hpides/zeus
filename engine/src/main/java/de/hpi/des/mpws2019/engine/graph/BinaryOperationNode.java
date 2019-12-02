package de.hpi.des.mpws2019.engine.graph;

import de.hpi.des.mpws2019.engine.operation.TwoInputOperator;

public class BinaryOperationNode<IN1, IN2, OUT> extends Node {

  @Override
  public void accept(NodeVisitor visitor) {
    visitor.visit(this);
  }

  private final TwoInputOperator<IN1, IN2, OUT> operator;

  public BinaryOperationNode(
      final TwoInputOperator<IN1, IN2, OUT> operator) {
    this.operator = operator;
  }

  public TwoInputOperator<IN1, IN2, OUT> getOperator() {
    return this.operator;
  }
}
