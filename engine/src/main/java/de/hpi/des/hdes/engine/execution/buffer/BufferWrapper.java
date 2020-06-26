package de.hpi.des.hdes.engine.execution.buffer;

import java.nio.ByteBuffer;

import lombok.Getter;

public class BufferWrapper {

    @Getter
    private final ByteBuffer writeBuffer;
    @Getter
    private final ReadBuffer readBuffer;
    private int limit;
    @Getter
    private boolean[] bitmask;
    @Getter
    private final int tupleSize;

    public BufferWrapper(final ByteBuffer writeBuffer, final ReadBuffer readBuffer, final int limit,
            final boolean[] bitmask, final int tupleSize) {
        this.writeBuffer = writeBuffer;
        this.readBuffer = readBuffer;
        this.limit = limit;
        this.bitmask = bitmask;
        this.tupleSize = tupleSize;
    }

    public boolean hasRemaining(final int bytes) {
        if (limit > writeBuffer.position()) {
            return writeBuffer.limit() - writeBuffer.position() > bytes;
        }
        return writeBuffer.limit() - writeBuffer.position() + limit > bytes;
    }

    public void free(final int offset) {
        int index = offset / tupleSize;
        bitmask[index] = false;
        if (this.limit + 1 == index) {
            while (!bitmask[index] && index != limit) {
                index++;
                if (index == writeBuffer.capacity()) {
                    index = 0;
                }
            }
            limit = index;
            if (limit > writeBuffer.position()) {
                writeBuffer.limit(limit);
            } else if (limit < writeBuffer.position()) {
                writeBuffer.limit(writeBuffer.capacity());
            }
        }
    }

    public void resetLimit() {
        writeBuffer.limit(this.limit);
    }
}