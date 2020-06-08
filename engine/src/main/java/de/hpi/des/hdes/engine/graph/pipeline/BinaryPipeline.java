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
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }

    private boolean isLeft(Node operatorNode) {
        // TODO evaluate if a list search or hash map makes more sense
        return leftNodes.contains(operatorNode);
    }

    @Override
    public void addParent(Pipeline pipeline, Node childNode) {
        if (this.isLeft(childNode) || (childNode.equals(binaryNode) && rightParent != null)) {
            this.leftParent = pipeline;
        } else {
            this.rightParent = pipeline;
        }
        pipeline.setChild(this);
    }

    @Override
    public void addOperator(Node operator, Node childNode) {
        if ((this.isLeft(childNode) && !childNode.equals(binaryNode)) || leftNodes.size() == 0) {
            this.leftNodes.add(operator);
        } else {
            this.rightNodes.add(operator);
        }
    }
}