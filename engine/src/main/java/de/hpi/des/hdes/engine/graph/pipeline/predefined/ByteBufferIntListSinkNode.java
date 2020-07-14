package de.hpi.des.hdes.engine.graph.pipeline.predefined;

import java.util.List;

import org.jooq.lambda.tuple.Tuple4;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.NodeVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.pipeline.node.GenerationNode;

public class ByteBufferIntListSinkNode extends GenerationNode {
    private final List<Tuple4<Long, Integer, Integer, Boolean>> resultList;

    public ByteBufferIntListSinkNode(List<Tuple4<Long, Integer, Integer, Boolean>> resultList) {
        super(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT },
                new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT }, null);
        this.resultList = resultList;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        // TODO Auto-generated method stub

    }

    @Override
    public void accept(PipelineTopology pipelineTopology) {
        Pipeline sinkPipeline = new ByteBufferIntListSinkPipeline(resultList);
        pipelineTopology.addPipelineAsLeaf(sinkPipeline, this);
    }

}
