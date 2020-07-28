package de.hpi.des.hdes.engine.graph.pipeline;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.io.Buffer;
import de.hpi.des.hdes.engine.graph.pipeline.node.NetworkSourceNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NetworkSourcePipeline extends Pipeline {

    @Getter
    private final NetworkSourceNode sourceNode;

    public NetworkSourcePipeline(NetworkSourceNode sourceNode) {
        super(sourceNode.getOutputTypes());
        this.sourceNode = sourceNode;
    }

    @Override
    public int hashCode() {
        return Math.abs(sourceNode.getHost().concat(Integer.toString(sourceNode.getPort())).hashCode());
    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public void loadPipeline(Dispatcher dispatcher, Class childKlass) {
        this.compileClass();
        try {
            pipelineObject = (Runnable) pipelineKlass.getDeclaredConstructor(Dispatcher.class).newInstance(dispatcher);// dispatcher.getReadByteBufferForPipeline(this),
                                                                                                                       // dispatcher,
                                                                                                                       // sourceNode.getHost(),
                                                                                                                       // sourceNode.getPort());
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
        return "source".concat(Integer.toString(hashCode()));
    }

}