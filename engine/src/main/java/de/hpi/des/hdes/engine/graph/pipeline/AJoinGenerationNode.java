package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.generators.AJoinGenerator;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AJoinGenerationNode extends GenerationNode {

    @Getter
    private final AJoinGenerator operator;

    public AJoinGenerationNode(final AJoinGenerator operator) {
        this.operator = operator;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO
    }

    @Override
    public void accept(PipelineTopology pipelineTopology) {
        AJoinPipeline pipeline = new AJoinPipeline(this);
        pipelineTopology.addPipelineAsLeaf(pipeline, this);
    }
}