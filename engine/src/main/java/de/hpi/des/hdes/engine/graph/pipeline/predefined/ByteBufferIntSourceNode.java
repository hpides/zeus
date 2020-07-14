package de.hpi.des.hdes.engine.graph.pipeline.predefined;

import java.util.List;

import org.jooq.lambda.tuple.Tuple2;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;

public class ByteBufferIntSourceNode extends GenerationNode {

    private final List<Tuple2<Integer, Boolean>> source;

    public ByteBufferIntSourceNode(List<Tuple2<Integer, Boolean>> source, final int outputTupleSize) {
        super(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT },
                new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT }, null);
        this.source = source;
    }

    @Override
    public void accept(PipelineTopology pipelineTopology) {
        pipelineTopology.addPipelineAsParent(new ByteBufferIntSourcePipeline(source), this);
    }

    @Override
    public void accept(NodeVisitor visitor) {
    }

}