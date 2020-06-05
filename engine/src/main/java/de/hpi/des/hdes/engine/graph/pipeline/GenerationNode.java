package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.graph.Node;

public abstract class GenerationNode extends Node {

    protected GenerationNode() {
        super();
    }

    protected GenerationNode(String string) {
        super(string);
    }

    public abstract void accept(PipelineTopology pipelineTopology);

}