package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.NodeVisitor;

public class BinaryPipelineNode<Left extends Pipeline, Right extends Pipeline, OUT> extends Node {
    private Pipeline leftPipeline;
    private Pipeline rightPipeline;

    BinaryPipelineNode(Pipeline leftPipeline, Pipeline rightPipeline) {
        this.leftPipeline = leftPipeline;
        this.rightPipeline = rightPipeline;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO Auto-generated method stub

    }
}