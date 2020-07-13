package de.hpi.des.hdes.engine.execution;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import de.hpi.des.hdes.engine.Query;
import de.hpi.des.hdes.engine.VulcanoEngine;
import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.execution.buffer.ReadBuffer;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSource;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSourcePipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;
import de.hpi.des.hdes.engine.io.Buffer;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opentest4j.AssertionFailedError;

public class DispatcherTest {
    
    private BufferedSource source = new BufferedSource(){
        @Override
        public Buffer getInputBuffer() {
            // TODO Auto-generated method stub
            return null;
        }
    };

    @Test
    void writeToBuffer() {
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        builder.streamOfC(new PrimitiveType[]{}, source).filter(new PrimitiveType[]{}, "() -> true");
        final PipelineTopology topology = PipelineTopology.pipelineTopologyOf(builder.buildAsQuery());
        final Dispatcher dispatcher = new Dispatcher(topology);

        byte[] bytes = {10, 11, 12};
        BufferedSourcePipeline pipeline = (BufferedSourcePipeline) topology.getPipelines().get(1);
        dispatcher.write(pipeline.getPipelineId(), bytes);
        
        UnaryPipeline unaryPipeline = (UnaryPipeline) topology.getPipelines().get(0);
        ReadBuffer readBuffer = dispatcher.getReadByteBufferForPipeline(unaryPipeline);
        byte[] result = new byte[3];
        readBuffer.getBuffer().get(result);
        assertArrayEquals(result, bytes);
    }
}
