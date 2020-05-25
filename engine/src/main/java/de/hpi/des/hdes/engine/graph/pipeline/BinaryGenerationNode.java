package de.hpi.des.hdes.engine.graph;

import de.hpi.des.hdes.engine.generators.JoinGenerator;
import lombok.Getter;

public class BinaryGenerationNode<IN1, IN2, OUT> extends Node {

    @Getter
    private final JoinGenerator<IN1, IN2, OUT> operator;

    public BinaryGenerationNode(final JoinGenerator<IN1, IN2, OUT> operator) {
        this.operator = operator;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO Auto-generated method stub

    }
}
