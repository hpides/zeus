package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.ArrayList;
import java.util.List;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.operation.Operator;
import lombok.Getter;

@Getter
public abstract class BinaryPipeline extends Pipeline {

    protected final List<Node> leftNodes;
    protected final List<Node> rightNodes;
    protected Node binaryNode;
    protected Pipeline leftParent;
    protected Pipeline rightParent;

    protected BinaryPipeline(List<Node> leftNodes, List<Node> rightNodes, Node binaryNode) {
        super();
        this.binaryNode = binaryNode;
        this.leftNodes = leftNodes;
        this.rightNodes = rightNodes;
    }

    protected BinaryPipeline(Node binaryNode) {
        super();
        this.binaryNode = binaryNode;
        this.leftNodes = new ArrayList<>();
        this.rightNodes = new ArrayList<>();
    }

    protected boolean isLeft(Node operatorNode) {
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