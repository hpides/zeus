package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import lombok.Getter;

@Getter
public class FileSinkPipeline extends SinkPipeline {
    private Pipeline parent;
    final private int writeEveryX;

    public FileSinkPipeline(PrimitiveType[] inputTypes, int writeEveryX) {
        super(inputTypes);
        this.writeEveryX = writeEveryX;
    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }

}