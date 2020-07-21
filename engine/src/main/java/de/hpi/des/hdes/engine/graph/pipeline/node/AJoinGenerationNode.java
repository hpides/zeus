package de.hpi.des.hdes.engine.graph.pipeline.node;

import de.hpi.des.hdes.engine.generators.AJoinGenerator;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.AJoinPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.generators.PrimitiveType;

public class AJoinGenerationNode extends BinaryGenerationNode {

    public AJoinGenerationNode(PrimitiveType[] inputTypes, PrimitiveType[] joinTypes, AJoinGenerator operator) {
        super(inputTypes, joinTypes, operator);
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO
    }

    @Override
    public void accept(PipelineTopology pipelineTopology) {
        AJoinPipeline pipeline = new AJoinPipeline(this);
        pipelineTopology.addPipelineAsParent(pipeline, this);
    }
}