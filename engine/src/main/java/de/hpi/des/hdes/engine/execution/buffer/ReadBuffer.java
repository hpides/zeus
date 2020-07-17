package de.hpi.des.hdes.engine.execution.buffer;

import java.nio.ByteBuffer;

import lombok.Getter;

@Getter
public class ReadBuffer {

    private ByteBuffer buffer;
    private String pipelineID;
    private int pipelineBufferIndex;
    private int mark;
    private int limit = 0;

    public ReadBuffer(ByteBuffer buffer, String pipelineID, int pipelineBufferIndex) {
        this.buffer = buffer;
        this.pipelineID = pipelineID;
        this.pipelineBufferIndex = pipelineBufferIndex;
    }

    public void mark() {
        mark = buffer.position();
    }

    public void reset() {
        buffer.position(mark);
    }

    public boolean hasRemaining() {
        return buffer.position() < limit;
    }

    public int remainingBytes() {
        return limit - buffer.position();
    }

    public void limit(int newLimit) {
        limit = newLimit;
    }

    public int limit() {
        return limit;
    }
}
