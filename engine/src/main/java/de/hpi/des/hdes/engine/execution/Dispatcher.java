package de.hpi.des.hdes.engine.execution;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.hpi.des.hdes.engine.execution.buffer.BufferWrapper;
import de.hpi.des.hdes.engine.execution.buffer.ReadBuffer;
import de.hpi.des.hdes.engine.graph.pipeline.BinaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.SinkPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;
import lombok.Getter;
import lombok.experimental.Accessors;

public class Dispatcher {

    // TODO Should be saved per buffer during initialization
    private static int NUMBER_OF_VECTORS = 10000;
    @Accessors(fluent = true)
    @Getter
    private static int TUPLES_PER_VECTOR = 900;
    @Accessors(fluent = true)
    @Getter
    private static int TUPLES_PER_READ_VECTOR = 100;

    private final Map<String, BufferWrapper> writeBuffers = new HashMap<>();
    private final Map<ReadBuffer, BufferWrapper> readBufferToBufferWrapper = new HashMap<>();

    public Dispatcher(final List<Pipeline> pipelines) {
        prepareBufferWrapper(pipelines);
    }

    private void prepareBufferWrapper(final List<Pipeline> pipelines) {
        for (final Pipeline pipeline : pipelines) {
            if (pipeline instanceof SinkPipeline) {
                continue;
            }
            int outputEventSize = 8 + pipeline.getOutputTupleLength() + 1;
            final ByteBuffer buffer = ByteBuffer
                    .allocateDirect(outputEventSize * NUMBER_OF_VECTORS * TUPLES_PER_VECTOR);
            final ByteBuffer readBuffer = buffer.asReadOnlyBuffer();
            final BufferWrapper bufferWrapper = new BufferWrapper(pipeline.getPipelineId(), buffer,
                    pipeline.getChild().getPipelineId(), new ReadBuffer(readBuffer), buffer.limit() / outputEventSize,
                    NUMBER_OF_VECTORS * TUPLES_PER_VECTOR, outputEventSize);
            writeBuffers.put(pipeline.getPipelineId(), bufferWrapper);
            readBufferToBufferWrapper.put(bufferWrapper.getReadBuffer(pipeline.getChild().getPipelineId()),
                    bufferWrapper);
        }
    }

    public void extend(List<Pipeline> newPipelines) {
        for (final Pipeline pipeline : newPipelines) {
            if (pipeline instanceof UnaryPipeline) {
                UnaryPipeline unaryPipeline = ((UnaryPipeline) pipeline);
                if (writeBuffers.containsKey(unaryPipeline.getParent().getPipelineId())) {
                    addReadBuffer(unaryPipeline.getParent().getPipelineId(), unaryPipeline.getPipelineId());
                }
            } else if (pipeline instanceof BinaryPipeline) {
                BinaryPipeline binaryPipeline = ((BinaryPipeline) pipeline);
                if (writeBuffers.containsKey(binaryPipeline.getLeftParent().getPipelineId())) {
                    addReadBuffer(binaryPipeline.getLeftParent().getPipelineId(), binaryPipeline.getPipelineId());
                }
                if (writeBuffers.containsKey(binaryPipeline.getRightParent().getPipelineId())) {
                    addReadBuffer(binaryPipeline.getRightParent().getPipelineId(), binaryPipeline.getPipelineId());
                }
            } else if (pipeline instanceof SinkPipeline) {
                SinkPipeline sinkPipeline = ((SinkPipeline) pipeline);
                if (writeBuffers.containsKey(sinkPipeline.getParent().getPipelineId())) {
                    addReadBuffer(sinkPipeline.getParent().getPipelineId(), sinkPipeline.getPipelineId());
                }
            }
        }
        prepareBufferWrapper(newPipelines);
    }

    private void addReadBuffer(final String parentPipelineID, final String childPipelineID) {
        BufferWrapper bufferWrapper = writeBuffers.get(parentPipelineID);
        ReadBuffer readBuffer = bufferWrapper.addReadBuffer(childPipelineID);
        readBufferToBufferWrapper.put(readBuffer, bufferWrapper);
    }

    public ReadBuffer getReadByteBufferForPipeline(final UnaryPipeline pipeline) {
        return writeBuffers.get(pipeline.getParent().getPipelineId()).getReadBuffer(pipeline.getPipelineId());
    }

    public ReadBuffer getReadByteBufferForPipeline(SinkPipeline pipeline) {
        return writeBuffers.get(pipeline.getParent().getPipelineId()).getReadBuffer(pipeline.getPipelineId());
    }

    public ReadBuffer getLeftByteBufferForPipeline(final BinaryPipeline pipeline) {
        return writeBuffers.get(pipeline.getLeftParent().getPipelineId()).getReadBuffer(pipeline.getPipelineId());
    }

    public ReadBuffer getRightByteBufferForPipeline(final BinaryPipeline pipeline) {
        return writeBuffers.get(pipeline.getRightParent().getPipelineId()).getReadBuffer(pipeline.getPipelineId());
    }

    public boolean write(final String pipeline, final byte[] bytes) {
        final BufferWrapper bufferWrapper = writeBuffers.get(pipeline);
        if (bufferWrapper.hasRemaining(bytes.length)) {

            final ByteBuffer writeBuffer = bufferWrapper.getWriteBuffer();

            int index = writeBuffer.position() / bufferWrapper.getTupleSize();
            bufferWrapper.acquireAtomic();
            byte[] bitmask = bufferWrapper.getBitmask();
            for (int count = 0; count < bytes.length / bufferWrapper.getTupleSize(); count++, index++) {
                bitmask[index] = bufferWrapper.getNumberActiveReader();
            }
            bufferWrapper.releaseAtomic();
            int position = writeBuffer.put(bytes).position();
            for (ReadBuffer readBuffer : bufferWrapper.getChildPipelineToReadBuffer().values()) {
                int old_limit = readBuffer.limit();
                if (position > old_limit) {
                    readBuffer.limit(position);
                }
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

    public void resetReadLimit(final String pipelineID, final ReadBuffer readBuffer) {
        readBufferToBufferWrapper.get(readBuffer).resetReadLimit(pipelineID);
    }
}