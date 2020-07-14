package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.List;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.execution.buffer.ReadBuffer;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.pipeline.node.AJoinGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class AJoinPipeline extends BinaryPipeline {

    protected AJoinPipeline(List<GenerationNode> leftNodes, List<GenerationNode> rightNodes,
            GenerationNode binaryNode) {
        super(leftNodes, rightNodes, binaryNode);
    }

    public AJoinPipeline(GenerationNode binaryNode) {
        super(binaryNode);
    }

    public static AJoinPipeline of(List<GenerationNode> leftNodes, List<GenerationNode> rightNodes,
            GenerationNode binaryNode) {
        return new AJoinPipeline(leftNodes, rightNodes, binaryNode);
    }

    @Override
    public void loadPipeline(Dispatcher dispatcher, Class childKlass) {
        this.compileClass();
        this.setLoaded(true);
        try {
            pipelineObject = pipelineKlass.getDeclaredConstructor(ReadBuffer.class, ReadBuffer.class, Dispatcher.class)
                    .newInstance(dispatcher.getLeftByteBufferForPipeline((BinaryPipeline) this),
                            dispatcher.getRightByteBufferForPipeline((BinaryPipeline) this), dispatcher);
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.error("Slot had an exception during class load: ", e);
        }
    }

    @Override
    public void accept(PipelineVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public AJoinGenerationNode getBinaryNode() {
        return (AJoinGenerationNode) this.binaryNode;
    }
}