package de.hpi.des.hdes.engine.graph;

import de.hpi.des.hdes.engine.graph.pipeline.BinaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;

public interface PipelineVisitor {
    void visit(UnaryPipeline unaryPipeline);

    void visit(BinaryPipeline binaryPipeline);
}