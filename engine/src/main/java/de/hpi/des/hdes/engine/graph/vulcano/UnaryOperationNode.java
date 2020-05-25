package de.hpi.des.hdes.engine.graph.vulcano;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.operation.OneInputOperator;

/**
 * Represents a unary operation in the logical plan.
 *
 * @param <IN>  type of incoming elements
 * @param <OUT> type of outgoing elements
 */
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
