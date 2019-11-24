package de.hpi.des.mpws2019.engine.graph;

import de.hpi.des.mpws2019.engine.operation.BinaryOperator;

public class BinaryOperationNode<IN1, IN2, OUT> extends Node {

  private final BinaryOperator<IN1, IN2, OUT> operator;

  public BinaryOperationNode(
      final BinaryOperator<IN1, IN2, OUT> operator) {
    this.operator = operator;
  }

  public BinaryOperator<IN1, IN2, OUT> getOperator() {
    return this.operator;
  }
}
