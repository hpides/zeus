package de.hpi.des.hdes.engine.execution;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import de.hpi.des.hdes.engine.execution.buffer.BufferWrapper;
import de.hpi.des.hdes.engine.execution.buffer.ReadBuffer;
import de.hpi.des.hdes.engine.graph.pipeline.BinaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.pipeline.SinkPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.predefined.ByteBufferIntListSinkPipeline;
import lombok.Getter;
import lombok.experimental.Accessors;

public class Dispatcher {

    // TODO Should be saved per buffer during initialization
    private static int NUMBER_OF_VECTORS = 50000;
    @Accessors(fluent = true)
    @Getter
    private static int TUPLES_PER_VECTOR = 900;
    @Accessors(fluent = true)
    @Getter
    private static int TUPLES_PER_READ_VECTOR = 100;

    private final PipelineTopology pipelineTopology;
    private final Map<String, BufferWrapper> writeBuffers = new HashMap<>();
    private final Map<ReadBuffer, BufferWrapper> readBufferToBufferWrapper = new HashMap<>();

    // Used as a unique ID for buffers
    private int counter = 0;

    public Dispatcher(final PipelineTopology pipelineTopology) {
        this.pipelineTopology = pipelineTopology;
        prepare();
    }

    private void prepare() {
        for (final Pipeline pipeline : pipelineTopology.getPipelines()) {
            int outputTupleSize = pipeline.getOutputTupleLength() + 9;
            final ByteBuffer buffer = ByteBuffer
                    .allocateDirect(outputTupleSize * NUMBER_OF_VECTORS * TUPLES_PER_VECTOR);
            final ByteBuffer readBuffer = buffer.asReadOnlyBuffer();
            final BufferWrapper bufferWrapper = new BufferWrapper(buffer,
                    new ReadBuffer(readBuffer, pipeline.getPipelineId(), counter++), buffer.limit() / outputTupleSize,
                    new boolean[NUMBER_OF_VECTORS * TUPLES_PER_VECTOR], outputTupleSize);
            writeBuffers.put(pipeline.getPipelineId(), bufferWrapper);
            readBufferToBufferWrapper.put(bufferWrapper.getReadBuffer(), bufferWrapper);
        }
    }

    public ReadBuffer getReadByteBufferForPipeline(final UnaryPipeline pipeline) {
        return writeBuffers.get(pipeline.getParent().getPipelineId()).getReadBuffer();
    }

    public ReadBuffer getReadByteBufferForPipeline(SinkPipeline pipeline) {
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
            final ByteBuffer writeBuffer = bufferWrapper.getWriteBuffer();

            // loop over ever bytes array
            // for every bufferWrapper.getTupleSize() bytes set the index in the bitmask to
            // true
            int index = writeBuffer.position() / bufferWrapper.getTupleSize();
            boolean[] bitmask = bufferWrapper.getBitmask();
            for (int count = 0; count < bytes.length / bufferWrapper.getTupleSize(); count++, index++) {
                bitmask[index] = true;
            }
            int position = writeBuffer.put(bytes).position();
            int old_limit = bufferWrapper.getReadBuffer().limit();
            if (position > old_limit) {
                bufferWrapper.getReadBuffer().limit(position);
            }
            if (position == writeBuffer.capacity()) {
                bufferWrapper.resetWriteLimt();
            }
            return true;
        }
        return false;
    }

    public void free(final ReadBuffer readBuffer, final int[] offsets) {
        readBufferToBufferWrapper.get(readBuffer).free(offsets);
    }

    public void resetReadLimit(final ReadBuffer readBuffer) {
        readBufferToBufferWrapper.get(readBuffer).resetReadLimit();
    }
}