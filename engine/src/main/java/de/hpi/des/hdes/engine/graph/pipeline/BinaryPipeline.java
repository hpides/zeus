package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.operation.Operator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public abstract class BinaryPipeline extends Pipeline {
    protected final List<GenerationNode> leftNodes;
    protected final List<GenerationNode> rightNodes;
    protected GenerationNode binaryNode;
    protected Pipeline leftParent;
    protected Pipeline rightParent;
    final protected PrimitiveType[] joinInputTypes;

    protected BinaryPipeline(List<GenerationNode> leftNodes, List<GenerationNode> rightNodes,
            GenerationNode binaryNode) {
        super(leftNodes.get(leftNodes.size() - 1).getInputTypes());
        this.joinInputTypes = rightNodes.get(rightNodes.size() - 1).getInputTypes();
        this.binaryNode = binaryNode;
        this.leftNodes = leftNodes;
        this.rightNodes = rightNodes;
    }

    protected BinaryPipeline(GenerationNode binaryNode) {
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

    public String getPipelineId() {
        return "c"
                .concat(Integer
                        .toString(leftNodes.stream().map(t -> t.getNodeId()).collect(Collectors.joining()).hashCode()))
                .concat(Integer.toString(
                        rightNodes.stream().map(t -> t.getNodeId()).collect(Collectors.joining()).hashCode()));
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

    @Override
    public void replaceParent(Pipeline newParentPipeline) {
        if (this.leftParent.getPipelineId().equals(newParentPipeline.getPipelineId())) {
            this.leftParent = newParentPipeline;
        } else if (this.rightParent.getPipelineId().equals(newParentPipeline.getPipelineId())) {
            this.rightParent = newParentPipeline;
        } else {
            log.error("Tried replace parent in binary pipeline with pipelineID {} but found no matching parent",
                    newParentPipeline.getPipelineId());
        }
        newParentPipeline.setChild(this);
    }

    @Override
    public int getOutputTupleLength() {
        int length = 0;
        for (PrimitiveType pt : binaryNode.getOutputTypes()) {
            length += pt.getLength();
        }
        return length;
    }
}