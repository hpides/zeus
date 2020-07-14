package de.hpi.des.hdes.engine.graph.pipeline.node;

import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSourcePipeline;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSource;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import lombok.Getter;

public class BufferedSourceNode extends GenerationNode {

    @Getter
    private final BufferedSource source;

    public BufferedSourceNode(PrimitiveType[] outputTypes, BufferedSource source) {
        super(null, outputTypes, null);
        this.source = source;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO Auto-generated method stub

    }

    @Override
    public void accept(PipelineTopology pipelineTopology) {
        BufferedSourcePipeline sourcePipeline = new BufferedSourcePipeline(this);
        pipelineTopology.addPipelineAsParent(sourcePipeline, this);
    }

}
