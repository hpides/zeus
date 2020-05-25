package de.hpi.des.hdes.engine.graph.vulcano;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.operation.TwoInputOperator;

/**
 * Represents binary operations in the logical plan
 *
 * @param <IN1> type of elements in the first stream
 * @param <IN2> type of elements in the second stream
 * @param <OUT> type of elements in the resulting stream
 */
public class BinaryOperationNode<IN1, IN2, OUT> extends Node {

  @Override
  public void accept(final NodeVisitor visitor) {
    visitor.visit(this);
  }

  private final TwoInputOperator<IN1, IN2, OUT> operator;

  public BinaryOperationNode(final TwoInputOperator<IN1, IN2, OUT> operator) {
    this.operator = operator;
  }

  public BinaryOperationNode(final String identifier, final TwoInputOperator<IN1, IN2, OUT> operator) {
    super(identifier);
    this.operator = operator;
  }

  public TwoInputOperator<IN1, IN2, OUT> getOperator() {
    return this.operator;
  }
}
