package de.hpi.des.hdes.engine.execution;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import de.hpi.des.hdes.engine.graph.pipeline.Pipeline;
import de.hpi.des.hdes.engine.graph.pipeline.PipelineTopology;

public class Dispatcher {

    private static int BUFFER_SIZE = 500;

    private final PipelineTopology pipelineTopology;
    private final Map<Pipeline, ByteBuffer> writeBuffers = new HashMap<>();
    private final Map<ByteBuffer, ByteBuffer> readToWriteBuffer = new HashMap<>();
    private final Map<ByteBuffer, Integer> writeBufferToLimit = new HashMap<>();

    public Dispatcher(final PipelineTopology pipelineTopology) {
        this.pipelineTopology = pipelineTopology;
        prepare();
    }

    private void prepare() {
        for (final Pipeline pipeline : pipelineTopology.getPipelines()) {
            final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
            writeBuffers.put(pipeline, buffer);
            writeBufferToLimit.put(buffer, buffer.limit());

            final ByteBuffer readBuffer = buffer.asReadOnlyBuffer();
            readToWriteBuffer.put(readBuffer, buffer);
        }
    }

    public ByteBuffer getReadByteBufferForPipeline(final Pipeline pipeline) {
        return readToWriteBuffer.get(writeBuffers.get(pipeline));
    }

    public boolean write(final Pipeline pipeline, final byte[] bytes) {
        final ByteBuffer pipelineBuffer = writeBuffers.get(pipeline);
        if (hasRemaining(pipelineBuffer, bytes.length)) {
            pipelineBuffer.put(bytes);
            return true;
        }
        return false;
    }

    private boolean hasRemaining(final ByteBuffer writeBuffer, final int bytes) {
        if (writeBufferToLimit.get(writeBuffer) > writeBuffer.position()) {
            return writeBuffer.limit() - writeBuffer.position() > bytes;
        }
        return writeBuffer.limit() - writeBuffer.position() + writeBufferToLimit.get(writeBuffer) > bytes;
    }

    public void free(final ByteBuffer readBuffer, final int offset) {
        final ByteBuffer writeBuffer = readToWriteBuffer.get(readBuffer);
        if (offset <= writeBuffer.limit()) {
            writeBuffer.limit(writeBuffer.capacity());
        } else {
            writeBuffer.limit(offset);
        }
        writeBufferToLimit.put(writeBuffer, offset);
    }

    public void resetLimit(final ByteBuffer readBuffer) {
        final ByteBuffer writeBuffer = readToWriteBuffer.get(readBuffer);
        writeBuffer.limit(writeBufferToLimit.get(writeBuffer));
    }
}