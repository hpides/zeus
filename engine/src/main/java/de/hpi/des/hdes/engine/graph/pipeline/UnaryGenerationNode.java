package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.generators.FilterGenerator;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.NodeVisitor;

/**
 * Represents a unary operation in the logical plan.
 *
 * @param <IN>  type of incoming elements
 * @param <OUT> type of outgoing elements
 */
public class UnaryGenerationNode<IN, OUT> extends Node {

    private final FilterGenerator<IN> operator;

    public UnaryGenerationNode(final FilterGenerator<IN> operator) {
        super(operator.toString());
        this.operator = operator;
    }

    protected UnaryGenerationNode(final String identifier, final FilterGenerator<IN> operator) {
        super(identifier);
        this.operator = operator;
    }

    public FilterGenerator<IN> getOperator() {
        return this.operator;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO Auto-generated method stub

    }
}
