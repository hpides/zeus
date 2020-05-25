package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.generators.Generatable;

/**
 * Represents a unary operation in the logical plan.
 *
 * @param <IN>  type of incoming elements
 * @param <OUT> type of outgoing elements
 */
public class UnaryGenerationNode extends Node {

    private final Generatable operator;

    public UnaryGenerationNode(final Generatable operator) {
        super(operator.toString());
        this.operator = operator;
    }

    protected UnaryGenerationNode(final String identifier, final Generatable operator) {
        super(identifier);
        this.operator = operator;
    }

    public Generatable getOperator() {
        return this.operator;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO Auto-generated method stub

    }
}
