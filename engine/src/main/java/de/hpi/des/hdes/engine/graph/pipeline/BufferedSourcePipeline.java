package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.BufferedSourceNode;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.io.Buffer;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BufferedSourcePipeline extends Pipeline {

    @Getter
    private final BufferedSourceNode sourceNode;
    @Setter
    @Getter
    private Runnable pipelineObject;
    private boolean shutdownFlag;

    public BufferedSourcePipeline(BufferedSourceNode sourceNode) {
        super(new PrimitiveType[0]);
        this.sourceNode = sourceNode;
    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public void loadPipeline(Dispatcher dispatcher, Class childKlass) {
        this.compileClass();
        try {
            pipelineObject = (Runnable) pipelineKlass.getDeclaredConstructor(Buffer.class, childKlass)
                    .newInstance(sourceNode.getSource().getInputBuffer());
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.error("Slot had an exception during class load: ", e);
        }
    }

    @Override
    public void addParent(Pipeline pipeline, GenerationNode childNode) {
        this.setChild(pipeline);
    }

    @Override
    public void addOperator(GenerationNode operator, GenerationNode childNode) {
        // TODO Auto-generated method stub

    }

}