package de.hpi.des.hdes.engine.execution;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import de.hpi.des.hdes.engine.graph.pipeline.BinaryPipeline;
import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;
import de.hpi.des.hdes.engine.graph.pipeline.UnaryPipeline;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Dispatcher {

    private static int BUFFER_SIZE = 500;

    private final PipelineTopology pipelineTopology;
    private final Map<Pipeline, BufferWrapper> writeBuffers = new HashMap<>();
    private final Map<ByteBuffer, BufferWrapper> readBufferToBufferWrapper = new HashMap<>();

    private class BufferWrapper {
        
        @Getter
        private final ByteBuffer writeBuffer;
        @Getter
        private final ByteBuffer readBuffer;
        private int limit;

        public BufferWrapper(final ByteBuffer writeBuffer, final ByteBuffer readBuffer, final int limit) {
            this.writeBuffer = writeBuffer;
            this.readBuffer = readBuffer;
            this.limit = limit;
        }

        public boolean hasRemaining(final int bytes) {
            if (limit > writeBuffer.position()) {
                return writeBuffer.limit() - writeBuffer.position() > bytes;
            }
            return writeBuffer.limit() - writeBuffer.position() + limit > bytes;
        }

        public void free(final int offset) {
            if (offset <= writeBuffer.limit()) {
                writeBuffer.limit(writeBuffer.capacity());
            } else {
                writeBuffer.limit(offset);
            }
            this.limit = offset;
        }
    
        public void resetLimit() {
            writeBuffer.limit(this.limit);
        }
    }

    public Dispatcher(final PipelineTopology pipelineTopology) {
        this.pipelineTopology = pipelineTopology;
        prepare();
    }

    private void prepare() {
        for (final Pipeline pipeline : pipelineTopology.getPipelines()) {
            final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
            final ByteBuffer readBuffer = buffer.asReadOnlyBuffer();
            final BufferWrapper bufferWrapper = new BufferWrapper(buffer, readBuffer, buffer.limit());
            writeBuffers.put(pipeline, bufferWrapper);
            readBufferToBufferWrapper.put(readBuffer, bufferWrapper);
        }
    }

    public ByteBuffer getReadByteBufferForPipeline(final UnaryPipeline pipeline) {
        return writeBuffers.get(pipeline.getParent()).getReadBuffer();
    }

    public ByteBuffer getLeftByteBufferForPipeline(final BinaryPipeline pipeline) {
        return writeBuffers.get(pipeline.getLeftParent()).getReadBuffer();
    }

     public ByteBuffer getRightByteBufferForPipeline(final BinaryPipeline pipeline) {
         return writeBuffers.get(pipeline.getRightParent()).getReadBuffer();
    }
    
    public boolean write(final Pipeline pipeline, final byte[] bytes) {
        final BufferWrapper bufferWrapper = writeBuffers.get(pipeline);
        if (bufferWrapper.hasRemaining(bytes.length)) {
            final ByteBuffer pipelineBuffer = writeBuffers.get(pipeline).getWriteBuffer();
            pipelineBuffer.put(bytes);
            return true;
        }
        return false;
    }

    public void free(final ByteBuffer readBuffer, final int offset) {
        readBufferToBufferWrapper.get(readBuffer).free(offset);
    }

    public void resetLimit(final ByteBuffer readBuffer) {
        readBufferToBufferWrapper.get(readBuffer).resetLimit();        
    }
}