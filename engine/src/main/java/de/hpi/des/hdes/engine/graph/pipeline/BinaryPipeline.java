package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.pipeline.node.BinaryGenerationNode;
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
    protected PrimitiveType[] joinInputTypes;
    private final HashMap<String, MaterializationData> joinVariables = new HashMap<>();
    private final ArrayList<String> joinCurrentTypes = new ArrayList<>();

    protected BinaryPipeline(List<GenerationNode> leftNodes, List<GenerationNode> rightNodes,
            BinaryGenerationNode binaryNode) {
        super(leftNodes.size() == 0 ? binaryNode.getJoinInputTypes()
                : leftNodes.get(leftNodes.size() - 1).getInputTypes());
        this.joinInputTypes = rightNodes.size() == 0 ? binaryNode.getJoinInputTypes()
                : rightNodes.get(rightNodes.size() - 1).getInputTypes();
        this.binaryNode = binaryNode;
        this.leftNodes = leftNodes;
        this.rightNodes = rightNodes;
        for (int i = 0; i < joinInputTypes.length; i++)
            joinCurrentTypes.add(null);
    }

    protected BinaryPipeline(BinaryGenerationNode binaryNode) {
        super(binaryNode.getInputTypes());
        this.joinInputTypes = binaryNode.getJoinInputTypes();
        this.binaryNode = binaryNode;
        this.leftNodes = new ArrayList<>();
        this.rightNodes = new ArrayList<>();
        for (int i = 0; i < joinInputTypes.length; i++)
            joinCurrentTypes.add(null);
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
            this.inputTypes = childNode.getInputTypes();
        } else {
            this.rightNodes.add(operator);
            this.joinInputTypes = childNode.getInputTypes();
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
            return getVariableAtIndex(index, "leftInput");
        }
        String varName = joinCurrentTypes.get(index);
        if (varName != null) {
            return joinVariables.get(varName);
        }
        int offset = 8;
        for (int i = 0; i < index; i++) {
            offset += joinInputTypes[i].getLength();
        }
        MaterializationData var = new MaterializationData(joinVariables.size() + variables.size(), offset,
                joinInputTypes[index], "rightInput");
        joinCurrentTypes.set(index, var.getVarName());
        joinVariables.put(var.getVarName(), var);
        return var;
    }

    public void removeVariableAtIndex(int index, boolean isRight) {
        if (!isRight) {
            removeVariableAtIndex(index, "leftInput");
            return;
        }
        for (int i = index + 1; i < joinCurrentTypes.size(); i++) {
            getVariableAtIndex(i, isRight);
        }
        joinCurrentTypes.remove(index);
    }

    public MaterializationData addVariable(PrimitiveType type, boolean isRight) {
        if (!isRight) {
            return addVariable(type, "leftInput");
        }
        MaterializationData var = new MaterializationData(joinVariables.size() + variables.size(), type, "rightInput");
        joinCurrentTypes.add(var.getVarName());
        joinVariables.put(var.getVarName(), var);
        return var;
    }

    public MaterializationData[] getJoinVariables() {
        return this.joinVariables.values().toArray(new MaterializationData[joinVariables.size()]);
    }

    public String getWriteout(String bufferName, boolean isRight) {
        if (!isRight) {
            return getWriteout(bufferName);
        } // TODO Optimize Timestamp and watermark copy
        String implementation = "outputBuffer.position(initialOutputOffset+8);\n";
        int copyLength = 0;
        int arrayOffset = 8;
        for (int i = 0; i < joinCurrentTypes.size(); i++) {
            if (joinCurrentTypes.get(i) == null) {
                copyLength += joinInputTypes[i].getLength();
            } else {
                MaterializationData var = joinVariables.get(joinCurrentTypes.get(i));
                if (copyLength != 0) {
                    implementation = implementation.concat(bufferName)
                            .concat(".getBuffer().get(output, initialOutputOffset+")
                            .concat(Integer.toString(arrayOffset)).concat(", ").concat(Integer.toString(copyLength))
                            .concat(");\n");
                    implementation = implementation.concat("outputBuffer.position(outputBuffer.position()+")
                            .concat(Integer.toString(copyLength + arrayOffset)).concat(");\n");
                    copyLength = 0;
                }
                implementation = implementation.concat("outputBuffer.put").concat(var.getType().getUppercaseName())
                        .concat("(").concat(var.getVarName()).concat(");\n");
                arrayOffset += var.getType().getLength();
            }
        }
        if (copyLength != 0) {
            implementation = implementation.concat(bufferName).concat(".getBuffer().get(output, initialOutputOffset+")
                    .concat(Integer.toString(arrayOffset + copyLength)).concat(", ")
                    .concat(Integer.toString(copyLength).concat(");\n"));
            copyLength = 0;
        }
        return implementation;
    }
}