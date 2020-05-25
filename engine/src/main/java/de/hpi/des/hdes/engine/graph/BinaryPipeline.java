package de.hpi.des.hdes.engine.graph;

import java.util.List;

import lombok.Getter;

@Getter
public class BinaryPipeline extends Pipeline {

    private final List<Node> leftNodes;
    private final List<Node> rightNodes;

    protected BinaryPipeline(List<Node> leftNodes, List<Node> rightNodes) {
        super();
        this.leftNodes = leftNodes;
        this.rightNodes = rightNodes;
    }

    public static BinaryPipeline of(final List<Node> leftNodes, List<Node> rightNodes) {
        return new BinaryPipeline(leftNodes, rightNodes);
    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }
}