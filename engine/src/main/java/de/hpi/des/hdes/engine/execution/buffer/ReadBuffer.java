package de.hpi.des.hdes.engine.execution.buffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

import lombok.Getter;
import lombok.Setter;

@Getter
public class ReadBuffer {

    private ByteBuffer buffer;
    private int mark;
    private int limit = 0;
    @Setter
    private int freezePosition;

    public ReadBuffer(final ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public ReadBuffer(final ByteBuffer buffer, final int positioning) {
        this.buffer = buffer;
        this.limit = positioning;
        buffer.position(positioning);
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
