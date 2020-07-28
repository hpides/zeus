package de.hpi.des.hdes.engine.graph.pipeline.node;

import de.hpi.des.hdes.engine.generators.BinaryGeneratable;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.pipeline.JoinPipeline;

public class JoinGenerationNode extends BinaryGenerationNode {

    public JoinGenerationNode(PrimitiveType[] inputTypes, PrimitiveType[] ajoinTypes, BinaryGeneratable operator) {
        super(inputTypes, ajoinTypes, operator);
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO Auto-generated method stub

    }

    @Override
    public void accept(PipelineTopology pipelineTopology) {
        JoinPipeline pipeline = new JoinPipeline(this);
        pipelineTopology.addPipelineAsParent(pipeline, this);
    }
}
