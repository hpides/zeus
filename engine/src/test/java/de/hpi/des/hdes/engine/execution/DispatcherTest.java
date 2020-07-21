package de.hpi.des.hdes.engine.execution;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.VulcanoEngine;
import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.execution.buffer.ReadBuffer;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.predefined.ByteBufferIntSourcePipeline;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;
import de.hpi.des.hdes.engine.io.Buffer;

import java.nio.ByteBuffer;
import java.util.LinkedList;

import org.jooq.lambda.tuple.Tuple2;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opentest4j.AssertionFailedError;

public class DispatcherTest {

    @Test
    void writeToBuffer() {
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        builder.streamOfC(new LinkedList<Tuple2<Integer, Boolean>>(), 3).filter(new PrimitiveType[] {}, "() -> true");
        final PipelineTopology topology = PipelineTopology.pipelineTopologyOf(builder.buildAsQuery());
        final Dispatcher dispatcher = new Dispatcher(topology.getPipelines());

        byte[] bytes = { 10, 11, 12 };
        ByteBufferIntSourcePipeline pipeline = (ByteBufferIntSourcePipeline) topology.getPipelines().get(1);
        builder.streamOfC(new LinkedList<Tuple2<Integer, Boolean>>(), 3).filter(new PrimitiveType[] {}, "() -> true");

        dispatcher.write(pipeline.getPipelineId(), bytes);

        UnaryPipeline unaryPipeline = (UnaryPipeline) topology.getPipelines().get(0);
        ReadBuffer readBuffer = dispatcher.getReadByteBufferForPipeline(unaryPipeline);
        byte[] result = new byte[3];
        readBuffer.getBuffer().get(result);
        assertArrayEquals(result, bytes);
    }
}
