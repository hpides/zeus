package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.List;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.execution.buffer.ReadBuffer;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class AJoinPipeline extends BinaryPipeline {

    protected AJoinPipeline(List<Node> leftNodes, List<Node> rightNodes, Node binaryNode) {
        super(leftNodes, rightNodes, binaryNode);
    }

    protected AJoinPipeline(Node binaryNode) {
        super(binaryNode);
    }

    public static AJoinPipeline of(List<Node> leftNodes, List<Node> rightNodes, Node binaryNode) {
        return new AJoinPipeline(leftNodes, rightNodes, binaryNode);
    }

    @Override
    public void loadPipeline(Dispatcher dispatcher, Class childKlass) {
        this.compileClass();
        try {
            pipelineObject = pipelineKlass.getDeclaredConstructor(ReadBuffer.class, ReadBuffer.class, Dispatcher.class,
                    long.class, long.class).newInstance(dispatcher.getLeftByteBufferForPipeline((BinaryPipeline) this),
                            dispatcher.getRightByteBufferForPipeline((BinaryPipeline) this), dispatcher, 1000, 1000);
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