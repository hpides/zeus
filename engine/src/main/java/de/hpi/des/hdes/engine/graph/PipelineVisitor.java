package de.hpi.des.hdes.engine.graph;

import de.hpi.des.hdes.engine.graph.pipeline.AJoinPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.JoinPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.SinkPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSourcePipeline;
import de.hpi.des.hdes.engine.graph.pipeline.NetworkSourcePipeline;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;

public interface PipelineVisitor {
    void visit(UnaryPipeline unaryPipeline);

    void visit(AJoinPipeline binaryPipeline);

    void visit(JoinPipeline binaryPipeline);

    void visit(BufferedSourcePipeline sourcePipeline);

    void visit(NetworkSourcePipeline sourcePipeline);

    void visit(SinkPipeline sinkPipeline);
}
