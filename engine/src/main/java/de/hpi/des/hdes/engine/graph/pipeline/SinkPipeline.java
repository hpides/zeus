package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.execution.buffer.ReadBuffer;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;

@Slf4j
public abstract class SinkPipeline extends Pipeline {
    protected SinkPipeline(PrimitiveType[] types) {
        super(types);
    }

    @Getter
    private Pipeline parent;

    @Override
    public void loadPipeline(Dispatcher dispatcher, Class childKlass) {
        this.compileClass();
        try {
            pipelineObject = pipelineKlass.getDeclaredConstructor(ReadBuffer.class, Dispatcher.class)
                    .newInstance(dispatcher.getReadByteBufferForPipeline((SinkPipeline) this), dispatcher);
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.error("Slot had an exception during class load: ", e);
        }
    }

    @Override
    public void addParent(Pipeline pipeline, GenerationNode childNode) {
        this.parent = pipeline;
        pipeline.setChild(this);
    }

    @Override
    public void addOperator(GenerationNode operator, GenerationNode childNode) {
        log.warn("Tried to add operator {} with childe Node {} to a {} ({})", operator, childNode,
                this.getClass().getName(), getPipelineId());
    }

    @Override
    public void replaceParent(Pipeline newParentPipeline) {
        parent = newParentPipeline;
        newParentPipeline.setChild(this);
    }

}
