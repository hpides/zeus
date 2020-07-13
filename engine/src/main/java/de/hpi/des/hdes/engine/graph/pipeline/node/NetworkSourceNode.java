package de.hpi.des.hdes.engine.graph.pipeline.node;

import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.pipeline.NetworkSourcePipeline;
import lombok.Getter;

@Getter
public class NetworkSourceNode extends GenerationNode {

    private final String host;
    private final int port;
    

    public NetworkSourceNode(final PrimitiveType[] outputTypes, String host, int port) {
        super(null, outputTypes);
        this.host = host;
        this.port = port;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO Auto-generated method stub

    }

    @Override
    public void accept(PipelineTopology pipelineTopology) {
        NetworkSourcePipeline sourcePipeline = new NetworkSourcePipeline(this);
        pipelineTopology.addPipelineAsParent(sourcePipeline, this);
    }

}
