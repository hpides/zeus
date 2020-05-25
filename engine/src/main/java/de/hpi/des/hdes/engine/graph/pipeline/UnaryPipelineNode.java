package de.hpi.des.hdes.engine.graph.pipeline;

public class UnaryPipelineNode<IN extends Pipeline, OUT> {
    private Pipeline pipeline;

    UnaryPipelineNode(Pipeline pipeline) {
        this.pipeline = pipeline;
    }
}