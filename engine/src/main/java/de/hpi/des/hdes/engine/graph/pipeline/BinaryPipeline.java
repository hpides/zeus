package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.pipeline.node.BinaryGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;
import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.generators.templatedata.MaterializationData;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public abstract class BinaryPipeline extends Pipeline {
    protected final List<GenerationNode> rightNodes;
    protected BinaryGenerationNode binaryNode;
    protected Pipeline leftParent;
    protected Pipeline rightParent;
    protected PrimitiveType[] joinInputTypes;
    private final HashMap<String, MaterializationData> joinVariables = new HashMap<>();
    private final ArrayList<String> joinCurrentTypes = new ArrayList<>();
	private final String binaryPipelineId;

    protected BinaryPipeline(List<GenerationNode> leftNodes, List<GenerationNode> rightNodes,
            BinaryGenerationNode binaryNode) {
        super(leftNodes.size() == 0 ? binaryNode.getRightInputTypes()
                : leftNodes.get(leftNodes.size() - 1).getInputTypes(), leftNodes);
        this.joinInputTypes = rightNodes.size() == 0 ? binaryNode.getRightInputTypes()
                : rightNodes.get(rightNodes.size() - 1).getInputTypes();
        this.binaryNode = binaryNode;
        this.rightNodes = rightNodes;
        for (int i = 0; i < joinInputTypes.length; i++)
            joinCurrentTypes.add(null);
        this.binaryPipelineId = "c" + UUID.randomUUID().toString().replace("-", "");
    }

    protected BinaryPipeline(BinaryGenerationNode binaryNode) {
        super(binaryNode.getInputTypes(), new ArrayList<>());
        this.joinInputTypes = binaryNode.getRightInputTypes();
        this.binaryNode = binaryNode;
        this.rightNodes = new ArrayList<>();
        for (int i = 0; i < joinInputTypes.length; i++)
            joinCurrentTypes.add(null);
        this.binaryPipelineId = "c" + UUID.randomUUID().toString().replace("-", "");
    }

    public boolean isLeft(Node operatorNode) {
        if (operatorNode.equals(this.binaryNode.getRightParent())) {
            return false;
        }
        if (operatorNode.getParents().contains(operatorNode) || operatorNode.equals(this.binaryNode)) {
            return true;
        }
        return !operatorNode.getChildren().stream().anyMatch(n -> !this.isLeft(n));
    }

    private boolean isLeft(Pipeline pipeline, GenerationNode childNode) {
        if (childNode.equals(this.binaryNode)) {
            if (pipeline instanceof BinaryPipeline) {
                return !pipeline.getNodes().contains(((BinaryGenerationNode) childNode).getRightParent())
                        || !((BinaryPipeline) pipeline).getNodes()
                                .contains(((BinaryGenerationNode) childNode).getRightParent());
            }
            return !pipeline.getNodes().contains(((BinaryGenerationNode) childNode).getRightParent());
        }
        return nodes.contains(childNode);
    }

    public String getPipelineId() {
        return this.binaryPipelineId;
        // return "c"
        //         .concat(Integer.toString(
        //                 Math.abs(nodes.stream().map(t -> t.getNodeId()).collect(Collectors.joining()).hashCode())))
        //         .concat(Integer.toString(Math
        //                 .abs(rightNodes.stream().map(t -> t.getNodeId()).collect(Collectors.joining()).hashCode())));
    }

    @Override
    public void addParent(Pipeline pipeline, GenerationNode childNode) {
        if (this.isLeft(pipeline, childNode)) {
            this.leftParent = pipeline;
        } else {
            this.rightParent = pipeline;
        }
        pipeline.setChild(this);
    }

    @Override
    public void addOperator(GenerationNode operator, GenerationNode childNode) {
        if (this.isLeft(childNode)) {
            this.nodes.add(operator);
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
        int leftTupleSize = Stream.of(getInputTypes()).mapToInt(t -> t.getLength()).sum();
        String implementation = "outputBuffer.position(initialOutputOffset+8+ " + leftTupleSize + ");\n";
        int copyLength = 0;
        int arrayOffset = 8 + Stream.of(getInputTypes()).mapToInt(t -> t.getLength()).sum();
        for (int i = 0; i < joinCurrentTypes.size(); i++) {
            if (joinCurrentTypes.get(i) == null) {
                copyLength += joinInputTypes[i].getLength();
            } else {
                MaterializationData var = joinVariables.get(joinCurrentTypes.get(i));
                if (copyLength != 0) {
                    implementation = implementation.concat(bufferName).concat(".getBuffer().position(rightOffset+")
                            .concat(Integer.toString(arrayOffset - leftTupleSize)).concat(");\n").concat(bufferName)
                            .concat(".getBuffer().get(output, initialOutputOffset+")
                            .concat(Integer.toString(arrayOffset)).concat(", ").concat(Integer.toString(copyLength))
                            .concat(");\n");
                    implementation = implementation.concat("outputBuffer.position(outputBuffer.position()+")
                            .concat(Integer.toString(copyLength + arrayOffset)).concat(");\n");
                    arrayOffset += copyLength;
                    copyLength = 0;
                }
                implementation = implementation.concat("outputBuffer.put").concat(var.getType().getUppercaseName())
                        .concat("(").concat(var.getVarName()).concat(");\n");
                arrayOffset += var.getType().getLength();
            }
        }
        if (copyLength != 0) {
            implementation = implementation.concat(bufferName).concat(".getBuffer().position(rightOffset+")
                    .concat(Integer.toString(arrayOffset - leftTupleSize)).concat(");\n").concat(bufferName)
                    .concat(".getBuffer().get(output, initialOutputOffset+").concat(Integer.toString(arrayOffset))
                    .concat(", ").concat(Integer.toString(copyLength).concat(");\n"));
            copyLength = 0;
        }
        return implementation;
    }
}