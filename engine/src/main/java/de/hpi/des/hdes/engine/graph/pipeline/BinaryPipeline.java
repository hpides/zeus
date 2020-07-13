package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.ArrayList;
import java.util.List;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.operation.Operator;
import lombok.Getter;

@Getter
public abstract class BinaryPipeline extends Pipeline {
    protected final List<GenerationNode> leftNodes;
    protected final List<GenerationNode> rightNodes;
    protected Node binaryNode;
    protected Pipeline leftParent;
    protected Pipeline rightParent;
    final protected PrimitiveType[] joinInputTypes;

    protected BinaryPipeline(List<GenerationNode> leftNodes, List<GenerationNode> rightNodes, Node binaryNode) {
        super(leftNodes.get(leftNodes.size()-1).getInputTypes());
        this.joinInputTypes = rightNodes.get(rightNodes.size()-1).getInputTypes();
        this.binaryNode = binaryNode;
        this.leftNodes = leftNodes;
        this.rightNodes = rightNodes;
    }

    protected BinaryPipeline(Node binaryNode) {
        super(new PrimitiveType[0]);
        this.joinInputTypes = new PrimitiveType[0];
        this.binaryNode = binaryNode;
        this.leftNodes = new ArrayList<>();
        this.rightNodes = new ArrayList<>();
    }

    protected boolean isLeft(Node operatorNode) {
        // TODO evaluate if a list search or hash map makes more sense
        return leftNodes.contains(operatorNode);
    }

    @Override
    public void addParent(Pipeline pipeline, GenerationNode childNode) {
        if (this.isLeft(childNode) || (childNode.equals(binaryNode) && rightParent != null)) {
            this.leftParent = pipeline;
        } else {
            this.rightParent = pipeline;
        }
        pipeline.setChild(this);
    }

    @Override
    public void addOperator(GenerationNode operator, GenerationNode childNode) {
        if ((this.isLeft(childNode) && !childNode.equals(binaryNode)) || leftNodes.size() == 0) {
            this.leftNodes.add(operator);
        } else {
            this.rightNodes.add(operator);
        }
    }
}