package de.hpi.des.hdes.engine.graph;

import java.util.List;

import com.google.common.collect.Lists;

import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import lombok.Getter;

@Getter
public class BinaryPipeline extends Pipeline {

    private final List<Node> leftNodes;
    private final List<Node> rightNodes;
    private final BinaryGenerationNode binaryNode;

    protected BinaryPipeline(List<Node> leftNodes, List<Node> rightNodes, BinaryGenerationNode binaryNode) {
        super();
        this.leftNodes = leftNodes;
        this.rightNodes = rightNodes;
        this.binaryNode = binaryNode;
    }

    public static BinaryPipeline of(final List<Node> leftNodes, List<Node> rightNodes,
            BinaryGenerationNode binaryNode) {
        return new BinaryPipeline(leftNodes, rightNodes, binaryNode);
    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }
}