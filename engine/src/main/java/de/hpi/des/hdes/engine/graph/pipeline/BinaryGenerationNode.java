package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.generators.BinaryGeneratable;
import de.hpi.des.hdes.engine.generators.JoinGenerator;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BinaryGenerationNode extends GenerationNode {

    @Getter
    private final JoinGenerator operator;

    public BinaryGenerationNode(final BinaryGeneratable operator) {
        this.operator = (JoinGenerator) operator;
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
        } else {
            // TODO create new pipeline
            log.error("Used execution branch which is not implemented yet");
            pipelineTopology.addNodeToPipeline(this);
        }
    }
}
