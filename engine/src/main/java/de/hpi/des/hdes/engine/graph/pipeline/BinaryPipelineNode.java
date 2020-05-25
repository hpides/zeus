package de.hpi.des.hdes.engine.graph.pipeline;

public class BinaryPipelineNode <Left extends Pipeline, Right extends Pipeline, OUT> extends Node {
    private Pipeline leftPipeline;
    private Pipeline rightPipeline;

    BinaryPipelineNode(Pipeline leftPipeline, Pipeline rightPipeline) {
        this.leftPipeline = leftPipeline;
        this.rightPipeline = rightPipeline;
    }
}