package de.hpi.des.hdes.engine.graph.pipeline.predefined;

import java.util.List;
import java.util.UUID;

import org.jooq.lambda.tuple.Tuple2;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ByteBufferIntSourcePipeline extends Pipeline {

    private final List<Tuple2<Integer, Boolean>> source;

    public ByteBufferIntSourcePipeline(List<Tuple2<Integer, Boolean>> source) {
        super(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT });
        this.source = source;
    }

    @Override
    public void loadPipeline(Dispatcher dispatcher, Class childKlass) {
        pipelineObject = new ByteBufferIntSource(source, dispatcher, getPipelineId());
    }

    @Override
    public void accept(PipelineVisitor visitor) {
    }

    @Override
    public void addParent(Pipeline pipeline, GenerationNode childNode) {
        this.setChild(pipeline);
    }

    @Override
    public void addOperator(GenerationNode operator, GenerationNode childNode) {
        log.warn("Tried to add {} with childe Node {} to a {} ({})", operator, childNode, this.getClass().getName(),
                getPipelineId());
    }

    @Override
    public void replaceParent(Pipeline newParentPipeline) {
        log.error("Tried to replace parent of source pipeline {} ({}) with pipeline {} ({})", this,
                this.getPipelineId(), newParentPipeline, newParentPipeline.getPipelineId());
    }

    @Override
    public String getPipelineId() {
        return "c".concat(UUID.randomUUID().toString().replace("-", ""));
    }

}