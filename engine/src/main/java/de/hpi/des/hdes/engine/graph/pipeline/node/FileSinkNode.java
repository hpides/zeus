package de.hpi.des.hdes.engine.graph.pipeline.node;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.FileSinkPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import lombok.Getter;

public class FileSinkNode extends GenerationNode {

    @Getter
    final private int writeEveryX;

    public FileSinkNode(PrimitiveType[] types, int writeEveryX) {
        super(types, null, null);
        this.writeEveryX = writeEveryX;
    }

    @Override
    public void accept(PipelineTopology pipelineTopology) {
        Pipeline sinkPipeline = new FileSinkPipeline(inputTypes, writeEveryX);
        pipelineTopology.addPipelineAsLeaf(sinkPipeline, this);
    }

    @Override
    public void accept(NodeVisitor visitor) {
    }

}