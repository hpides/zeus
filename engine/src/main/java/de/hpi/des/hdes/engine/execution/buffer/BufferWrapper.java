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
        if (getLimitInBytes() > writeBuffer.position()) {
            return writeBuffer.limit() - writeBuffer.position() > bytes;
        }
        return writeBuffer.limit() - writeBuffer.position() + getLimitInBytes() > bytes;
    }

    public void free(final int offset) {
        int index = offset / tupleSize;
        bitmask[index] = false;
        int modLimit = limit % bitmask.length;
        if (modLimit == index) {
            boolean allFalse = false;
            do {
                index++;
                if (index == bitmask.length) {
                    index = 0;
                }
                if (index == modLimit) {
                    allFalse = true;
                    break;
                }
            } while (!bitmask[index]);
            if (allFalse) {
                limit = bitmask.length;
            } else {
                limit = index;
            }
            int writeLimit = getLimitInBytes();
            if (writeLimit > writeBuffer.position()) {
                writeBuffer.limit(writeLimit);
            } else if (writeLimit < writeBuffer.position()) {
                writeBuffer.limit(writeBuffer.capacity());
            }
        }
    }

    public void resetReadLimit() {
        readBuffer.getBuffer().position(0);
        readBuffer.limit(writeBuffer.position());
    }

    public void resetWriteLimt() {
        writeBuffer.position(0);
        if (limit != bitmask.length) {
            writeBuffer.limit(getLimitInBytes());
        } else {
            writeBuffer.limit(0);
        }
    }

    private int getLimitInBytes() {
        return limit * tupleSize;
    }
}