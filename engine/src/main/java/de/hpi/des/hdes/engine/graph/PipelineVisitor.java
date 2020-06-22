package de.hpi.des.hdes.engine.graph;

import de.hpi.des.hdes.engine.graph.pipeline.BinaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.SinkPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSourcePipeline;
import de.hpi.des.hdes.engine.graph.pipeline.NetworkSourcePipeline;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;

public interface PipelineVisitor {
    void visit(UnaryPipeline unaryPipeline);

    void visit(BinaryPipeline binaryPipeline);

    void visit(BufferedSourcePipeline sourcePipeline);

    void visit(NetworkSourcePipeline sourcePipeline);

    void visit(SinkPipeline sinkPipeline);
}
