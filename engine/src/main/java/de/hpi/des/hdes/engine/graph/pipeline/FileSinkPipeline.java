package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.UUID;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.node.FileSinkNode;
import lombok.Getter;

@Getter
public class FileSinkPipeline extends SinkPipeline {
    final private int writeEveryX;
    final private String pipelineId;

    public FileSinkPipeline(PrimitiveType[] inputTypes, int writeEveryX) {
        super(inputTypes);
        this.writeEveryX = writeEveryX;
        pipelineId = "c".concat(UUID.randomUUID().toString().replace("-", ""));
    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }

}