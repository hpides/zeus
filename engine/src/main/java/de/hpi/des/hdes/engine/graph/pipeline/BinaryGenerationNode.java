package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.generators.JoinGenerator;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import lombok.Getter;

public class BinaryGenerationNode extends GenerationNode {

    @Getter
    private final JoinGenerator operator;

    public BinaryGenerationNode(final JoinGenerator operator) {
        this.operator = operator;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO Auto-generated method stub

    }

    @Override
    public void accept(PipelineTopology pipelineTopology) {
        if (this.getChildren().isEmpty()) {
            BinaryPipeline currentPipeline = new BinaryPipeline(this);
            pipelineTopology.addPipelineAsLeaf(currentPipeline, this);
            currentPipeline.getLeftNodes().add(this);
            currentPipeline.getRightNodes().add(this);
        } else {
            pipelineTopology.addNodeToPipeline(this);
        }
    }
}
