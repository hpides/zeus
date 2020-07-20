package de.hpi.des.hdes.engine.graph.pipeline.predefined;

import java.util.List;
import java.util.UUID;

import org.jooq.lambda.tuple.Tuple4;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.PipelineVisitor;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.SinkPipeline;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ByteBufferIntListSinkPipeline extends SinkPipeline {
    private final List<Tuple4<Long, Integer, Integer, Boolean>> resultList;
    private final String pipelineId;

    public ByteBufferIntListSinkPipeline(List<Tuple4<Long, Integer, Integer, Boolean>> resultList) {
        super(new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT });
        this.resultList = resultList;
        pipelineId = "c".concat(UUID.randomUUID().toString().replace("-", ""));
    }

    @Override
    public void loadPipeline(Dispatcher dispatcher, Class childKlass) {
        pipelineObject = new ByteBufferIntListSink(dispatcher,
                dispatcher.getReadByteBufferForPipeline((SinkPipeline) this), resultList);
    }

    @Override
    public void accept(PipelineVisitor visitor) {
    }

    @Override
    public String getPipelineId() {
        return pipelineId;
    }

}