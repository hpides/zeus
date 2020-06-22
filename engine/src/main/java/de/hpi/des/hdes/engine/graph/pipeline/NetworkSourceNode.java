package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.graph.NodeVisitor;
import lombok.Getter;

@Getter
public class NetworkSourceNode extends GenerationNode {

    private final String host;
    private final int port;
    

    public NetworkSourceNode(String host, int port) {
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
