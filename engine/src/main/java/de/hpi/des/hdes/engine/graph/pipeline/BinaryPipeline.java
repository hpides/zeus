package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.generators.templatedata.MaterializationData;
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
    private final HashMap<String, MaterializationData> joinVariables = new HashMap<>();
    private final ArrayList<String> joinCurrentTypes = new ArrayList<>();

    protected BinaryPipeline(List<GenerationNode> leftNodes, List<GenerationNode> rightNodes,
            GenerationNode binaryNode) {
        // TODO ACCOunt for empty left and right nodes
        super(leftNodes.get(leftNodes.size() - 1).getInputTypes());
        this.joinInputTypes = rightNodes.get(rightNodes.size() - 1).getInputTypes();
        this.binaryNode = binaryNode;
        this.leftNodes = leftNodes;
        this.rightNodes = rightNodes;
        for (int i = 0; i < joinInputTypes.length; i++)
            joinCurrentTypes.add(null);
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
                .concat(Integer.toString(
                        Math.abs(leftNodes.stream().map(t -> t.getNodeId()).collect(Collectors.joining()).hashCode())))
                .concat(Integer.toString(Math
                        .abs(rightNodes.stream().map(t -> t.getNodeId()).collect(Collectors.joining()).hashCode())));
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

    public MaterializationData getVariableAtIndex(int index, boolean isRight) {
        if (!isRight) {
            return getVariableAtIndex(index);
        }
        String varName = joinCurrentTypes.get(index);
        if (varName != null) {
            return joinVariables.get(varName);
        }
        int offset = 0;
        for (int i = 0; i < index; i++) {
            offset += joinInputTypes[i].getLength();
        }
        MaterializationData var = new MaterializationData(joinVariables.size(), offset, joinInputTypes[index]);
        joinCurrentTypes.set(index, var.getVarName());
        joinVariables.put(var.getVarName(), var);
        return var;
    }

    public void removeVariableAtIndex(int index, boolean isRight) {
        if (!isRight) {
            removeVariableAtIndex(index);
            return;
        }
        for (int i = index + 1; i < joinCurrentTypes.size(); i++) {
            getVariableAtIndex(i, isRight);
        }
        joinCurrentTypes.remove(index);
    }

    public MaterializationData addVariable(PrimitiveType type, boolean isRight) {
        if (!isRight) {
            return addVariable(type, false);
        }
        MaterializationData var = new MaterializationData(joinVariables.size(), type);
        joinCurrentTypes.add(var.getVarName());
        joinVariables.put(var.getVarName(), var);
        return var;
    }

    public MaterializationData[] getJoinVariables() {
        return this.joinVariables.values().toArray(new MaterializationData[joinVariables.size()]);
    }
}