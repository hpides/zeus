package de.hpi.des.hdes.engine.execution;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import de.hpi.des.hdes.engine.execution.buffer.BufferWrapper;
import de.hpi.des.hdes.engine.execution.buffer.ReadBuffer;
import de.hpi.des.hdes.engine.graph.pipeline.BinaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Dispatcher {

    // TODO Should be saved per buffer during initialization
    private static int TUPLE_SIZE = 17; // size of one tuple in bytes
    private static int TUPLE_AMOUNT = 1000;

    private final PipelineTopology pipelineTopology;
    private final Map<String, BufferWrapper> writeBuffers = new HashMap<>();
    private final Map<ReadBuffer, BufferWrapper> readBufferToBufferWrapper = new HashMap<>();

    public Dispatcher(final PipelineTopology pipelineTopology) {
        this.pipelineTopology = pipelineTopology;
        prepare();
    }

    private void prepare() {
        for (final Pipeline pipeline : pipelineTopology.getPipelines()) {
            final ByteBuffer buffer = ByteBuffer.allocateDirect(TUPLE_SIZE * TUPLE_AMOUNT);
            final ByteBuffer readBuffer = buffer.asReadOnlyBuffer();
            final BufferWrapper bufferWrapper = new BufferWrapper(buffer,
                    new ReadBuffer(readBuffer, pipeline.getPipelineId(), 0), buffer.limit(), new boolean[TUPLE_AMOUNT],
                    TUPLE_SIZE);
            writeBuffers.put(pipeline.getPipelineId(), bufferWrapper);
            readBufferToBufferWrapper.put(bufferWrapper.getReadBuffer(), bufferWrapper);
        }
    }

    public ReadBuffer getReadByteBufferForPipeline(final UnaryPipeline pipeline) {
        return writeBuffers.get(pipeline.getParent().getPipelineId()).getReadBuffer();
    }

    public ReadBuffer getLeftByteBufferForPipeline(final BinaryPipeline pipeline) {
        return writeBuffers.get(pipeline.getLeftParent().getPipelineId()).getReadBuffer();
    }

    public ReadBuffer getRightByteBufferForPipeline(final BinaryPipeline pipeline) {
        return writeBuffers.get(pipeline.getRightParent().getPipelineId()).getReadBuffer();
    }

    public boolean write(final String pipeline, final byte[] bytes) {
        final BufferWrapper bufferWrapper = writeBuffers.get(pipeline);
        if (bufferWrapper.hasRemaining(bytes.length)) {
            final ByteBuffer pipelineBuffer = bufferWrapper.getWriteBuffer();
            int index = pipelineBuffer.position() / bufferWrapper.getTupleSize();
            int numberEvents = bytes.length / bufferWrapper.getTupleSize();
            boolean[] bitmask = bufferWrapper.getBitmask();
            while (numberEvents != 0) {
                numberEvents--;
                bitmask[index] = true;
                index++;
            }
            pipelineBuffer.put(bytes);
            return true;
        }
        return false;
    }

    public void free(final ReadBuffer readBuffer, final int offset) {
        readBufferToBufferWrapper.get(readBuffer).free(offset);
    }

    public void resetLimit(final ReadBuffer readBuffer) {
        readBufferToBufferWrapper.get(readBuffer).resetLimit();
    }
}