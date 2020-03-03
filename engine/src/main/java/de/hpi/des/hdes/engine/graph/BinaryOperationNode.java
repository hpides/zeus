package de.hpi.des.hdes.engine.graph;

import de.hpi.des.hdes.engine.operation.TwoInputOperator;

public class BinaryOperationNode<IN1, IN2, OUT> extends Node {

  @Override
  public void accept(final NodeVisitor visitor) {
    visitor.visit(this);
  }

  private final TwoInputOperator<IN1, IN2, OUT> operator;

  public BinaryOperationNode(final TwoInputOperator<IN1, IN2, OUT> operator) {
    this.operator = operator;
  }

  public BinaryOperationNode(final String identifier,
      final TwoInputOperator<IN1, IN2, OUT> operator) {
    super(identifier);
    this.operator = operator;
  }

  public TwoInputOperator<IN1, IN2, OUT> getOperator() {
    return this.operator;
  }
}
