package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.generators.JoinGenerator;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import lombok.Getter;

public class BinaryGenerationNode extends Node {

    @Getter
    private final JoinGenerator operator;

    public BinaryGenerationNode(final JoinGenerator operator) {
        this.operator = operator;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO Auto-generated method stub

    }
}
