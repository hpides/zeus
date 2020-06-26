package de.hpi.des.hdes.engine.generators;

import java.nio.ByteBuffer;

import de.hpi.des.hdes.engine.execution.buffer.ReadBuffer;
import org.junit.jupiter.api.Test;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.graph.pipeline.BinaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSink;
import de.hpi.des.hdes.engine.graph.pipeline.BufferedSource;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;
import de.hpi.des.hdes.engine.io.Buffer;
import de.hpi.des.hdes.engine.temp.ajoin.AJoin;

public class AJoinTest {
    // @Test
    // public void testAJoinWriteOutput() {
    // ByteBuffer leftInput = ByteBuffer.allocate(64);
    // ByteBuffer rightInput = ByteBuffer.allocate(64);
    // for (int i = 0; i < 4; i++) {
    // leftInput.putLong(i).putInt(i * 10).putInt(i * 10 + 1);
    // rightInput.putLong(i).putInt(i * 10).putInt(i * 10 + 1);
    // }
    // ByteBuffer output = ByteBuffer.allocate(256);
    // AJoin aJoin = new AJoin(leftInput, rightInput, output);
    // aJoin.writeOutput(24, 40);
    // }

    @Test
    public void testAJoinWindowing() {
        BufferedSource source = new BufferedSource() {

            @Override
            public Buffer getInputBuffer() {
                // TODO Auto-generated method stub
                return null;
            }
        };

        BufferedSink sink = new BufferedSink() {

            @Override
            public Buffer getOutputBuffer() {
                // TODO Auto-generated method stub
                return null;
            }
        };
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        var stream = builder.streamOfC(source);
        builder.streamOfC(source).ajoin(stream, new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT },
                new PrimitiveType[] { PrimitiveType.INT, PrimitiveType.INT }, 0, 0);
        PipelineTopology pt = PipelineTopology.pipelineTopologyOf(builder.build());
        Dispatcher dispatcher = new Dispatcher(pt);

        ReadBuffer leftInput = dispatcher.getLeftByteBufferForPipeline((BinaryPipeline) pt.getPipelines().get(0));
        ReadBuffer rightInput = dispatcher.getRightByteBufferForPipeline((BinaryPipeline) pt.getPipelines().get(0));

        for (int i = 1; i < 8; i++) {
            ByteBuffer input = ByteBuffer.allocate(17);
            input.putLong((long) i).putInt(i * 10).putInt(i * 10 + 1).put((byte)9);
            dispatcher.write(pt.getPipelines().get(1).getPipelineId(), input.array());
            dispatcher.write(pt.getPipelines().get(2).getPipelineId(), input.array());
        }
        ByteBuffer input = ByteBuffer.allocate(17);
        input.putLong((long) 8).putInt(8 * 10).putInt(8 * 10 + 1).put((byte)1);
        dispatcher.write(pt.getPipelines().get(1).getPipelineId(), input.array());
        dispatcher.write(pt.getPipelines().get(2).getPipelineId(), input.array());
        AJoin aJoin = new AJoin(leftInput, rightInput, dispatcher, 5, 5, pt.getPipelines().get(0).getPipelineId());
        for (int i = 0; i < 8; i++) {
            aJoin.readEventLeft();
            aJoin.readEventRight();
        }
        System.out.println("Done");
    }
}
