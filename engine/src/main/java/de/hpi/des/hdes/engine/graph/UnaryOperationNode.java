package de.hpi.des.hdes.engine.graph;

import de.hpi.des.hdes.engine.operation.OneInputOperator;

public class UnaryOperationNode<IN, OUT> extends Node {

  @Override
  public void accept(final NodeVisitor visitor) {
    visitor.visit(this);
  }

  private final OneInputOperator<IN, OUT> operator;

  public UnaryOperationNode(final OneInputOperator<IN, OUT> operator) {
    super(operator.toString());
    this.operator = operator;
  }

  protected UnaryOperationNode(final String identifier, final OneInputOperator<IN, OUT> operator) {
    super(identifier);
    this.operator = operator;
  }

  public OneInputOperator<IN, OUT> getOperator() {
    return this.operator;
  }
}
