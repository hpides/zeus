package de.hpi.des.hdes.engine.graph.pipeline.node;

import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.AggregationPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.generators.AggregateGenerator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AggregationGenerationNode extends GenerationNode {

    @Getter
    private final AggregateGenerator operator;

    public AggregationGenerationNode(final PrimitiveType[] inputTypes, PrimitiveType[] outputTypes, final AggregateGenerator operator) {
        super(inputTypes, outputTypes);
        this.operator = operator;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO
    }

    @Override
    public void accept(PipelineTopology pipelineTopology) {
        AggregationPipeline pipeline = new AggregationPipeline(this);
        pipelineTopology.addPipelineAsLeaf(pipeline, this);
    }
}