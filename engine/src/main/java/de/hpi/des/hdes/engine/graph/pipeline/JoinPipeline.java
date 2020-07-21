package de.hpi.des.hdes.engine.graph.pipeline;

import java.util.List;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.execution.buffer.ReadBuffer;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.node.JoinGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.BinaryGenerationNode;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class JoinPipeline extends BinaryPipeline {

    protected JoinPipeline(List<GenerationNode> leftNodes, List<GenerationNode> rightNodes,
            BinaryGenerationNode binaryNode) {
        super(leftNodes, rightNodes, binaryNode);
    }

    public JoinPipeline(BinaryGenerationNode binaryNode) {
        super(binaryNode);
    }

    public static JoinPipeline of(List<GenerationNode> leftNodes, List<GenerationNode> rightNodes,
            BinaryGenerationNode binaryNode) {
        return new JoinPipeline(leftNodes, rightNodes, binaryNode);
    }

    @Override
    public void loadPipeline(Dispatcher dispatcher, Class childKlass) {
        this.compileClass();
        this.setLoaded(true);
        try {
            pipelineObject = pipelineKlass.getDeclaredConstructor(ReadBuffer.class, ReadBuffer.class, Dispatcher.class,
                    long.class, long.class).newInstance(dispatcher.getLeftByteBufferForPipeline((BinaryPipeline) this),
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
    public JoinGenerationNode getBinaryNode() {
        return (JoinGenerationNode) this.binaryNode;
    }
}