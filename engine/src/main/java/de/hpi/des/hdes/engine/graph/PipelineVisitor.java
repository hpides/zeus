package de.hpi.des.hdes.engine.graph;

public interface PipelineVisitor {
    void visit(UnaryPipeline unaryPipeline);

    void visit(BinaryPipeline binaryPipeline);
}