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
    @Setter
    @Getter
    private Runnable pipelineObject;
    private boolean shutdownFlag;

    public NetworkSourcePipeline(NetworkSourceNode sourceNode) {
        super(sourceNode.getOutputTypes());
        this.sourceNode = sourceNode;
    }

    @Override
    public int hashCode() {
        return sourceNode.getHost().concat(Integer.toString(sourceNode.getPort())).hashCode();
    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public void loadPipeline(Dispatcher dispatcher, Class childKlass) {
        this.compileClass();
        this.setLoaded(true);
        try {
            pipelineObject = (Runnable) pipelineKlass.getDeclaredConstructor(Buffer.class, childKlass).newInstance();// dispatcher.getReadByteBufferForPipeline(this),
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
        // TODO Auto-generated method stub
    }

	@Override
	public void replaceParent(Pipeline newParentPipeline) {
		// TODO Auto-generated method stub
		
	}

}