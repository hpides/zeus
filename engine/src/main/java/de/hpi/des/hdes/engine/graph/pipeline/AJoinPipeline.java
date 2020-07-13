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

    protected AJoinPipeline(List<GenerationNode> leftNodes, List<GenerationNode> rightNodes, Node binaryNode) {
        super(leftNodes, rightNodes, binaryNode);
    }

    public AJoinPipeline(Node binaryNode) {
        super(binaryNode);
    }

    public static AJoinPipeline of(List<GenerationNode> leftNodes, List<GenerationNode> rightNodes, Node binaryNode) {
        return new AJoinPipeline(leftNodes, rightNodes, binaryNode);
    }

    @Override
    public void loadPipeline(Dispatcher dispatcher, Class childKlass) {
        this.compileClass();
        this.setLoaded(true);
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

    @Override
    public void addParent(Pipeline pipeline, GenerationNode childNode) {
      // TODO Auto-generated method stub
    }
  
    @Override
    public void addOperator(GenerationNode operator, GenerationNode childNode) {
      // TODO Auto-generated method stub
      
    }
}