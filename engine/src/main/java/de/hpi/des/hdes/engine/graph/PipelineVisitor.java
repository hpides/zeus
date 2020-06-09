package de.hpi.des.hdes.engine.graph;

import de.hpi.des.hdes.engine.graph.pipeline.BinaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.SinkPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.SourcePipeline;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;

public interface PipelineVisitor {
    void visit(UnaryPipeline unaryPipeline);

    void visit(BinaryPipeline binaryPipeline);

    void visit(SourcePipeline sourcePipeline);

    void visit(SinkPipeline sinkPipeline);
}
