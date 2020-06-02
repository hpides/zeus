package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import lombok.Getter;

@Getter
public class BinaryPipeline extends Pipeline {

    private final List<Node> leftNodes;
    private final List<Node> rightNodes;
    private final BinaryGenerationNode binaryNode;
    private Pipeline leftParent;
    private Pipeline rightParent;

    protected BinaryPipeline(List<Node> leftNodes, List<Node> rightNodes, BinaryGenerationNode binaryNode) {
        super();
        this.leftNodes = leftNodes;
        this.rightNodes = rightNodes;
        this.binaryNode = binaryNode;
    }

    protected BinaryPipeline(BinaryGenerationNode binaryNode) {
        super();
        this.binaryNode = binaryNode;
        this.leftNodes = new ArrayList<>();
        this.rightNodes = new ArrayList<>();
    }

    public static BinaryPipeline of(final List<Node> leftNodes, List<Node> rightNodes,
            BinaryGenerationNode binaryNode) {
        return new BinaryPipeline(leftNodes, rightNodes, binaryNode);
    }

    @Override
    public void addLeftParent(Pipeline leftPipeline) {
        super.addLeftParent(leftPipeline);
        this.leftParent = leftPipeline;
    }

    @Override
    public void addRightParent(Pipeline rightPipeline) {
        super.addRightParent(rightPipeline);
        this.rightParent = rightPipeline;
    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }
}