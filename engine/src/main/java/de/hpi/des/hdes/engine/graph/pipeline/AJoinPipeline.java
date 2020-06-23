package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.ArrayList;
import java.util.List;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import lombok.Getter;

@Getter
public class AJoinPipeline extends BinaryPipeline {

    protected AJoinPipeline(List<Node> leftNodes, List<Node> rightNodes, Node binaryNode) {
        super(leftNodes, rightNodes, binaryNode);
    }

    protected AJoinPipeline(Node binaryNode) {
        super(binaryNode);
    }

    public static AJoinPipeline of(List<Node> leftNodes, List<Node> rightNodes, Node binaryNode) {
        return new AJoinPipeline(leftNodes, rightNodes, binaryNode);
    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public AJoinGenerationNode getBinaryNode() {
        return (AJoinGenerationNode) this.binaryNode;
    }

}