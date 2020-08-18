package de.hpi.des.hdes.engine.execution.buffer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.Getter;

public class BufferWrapper {

    @Getter
    private final String pipelineID;
    @Getter
    private final ByteBuffer writeBuffer;
    @Getter
    private final Map<String, ReadBuffer> childPipelineToReadBuffer = new HashMap<>();
    @Getter
    private byte numberActiveReader;
    private int limit;
    @Getter
    private byte[] bitmask;
    @Getter
    private final int tupleSize;
    private final AtomicBoolean atomic = new AtomicBoolean(false);

    public BufferWrapper(final String pipelineID, final ByteBuffer writeBuffer, final String childPipelineID,
            final ReadBuffer readBuffer, final int limit, final int size, final int tupleSize) {
        this.writeBuffer = writeBuffer;
        childPipelineToReadBuffer.put(childPipelineID, readBuffer);
        this.limit = limit;
        this.bitmask = new byte[size];
        this.tupleSize = tupleSize;
        this.pipelineID = pipelineID;
        this.numberActiveReader = 1;
    }

    public boolean hasRemaining(final int bytes) {
        return writeBuffer.limit() - writeBuffer.position() >= bytes;
    }

    public ReadBuffer getReadBuffer(String pipelineID) {
        return childPipelineToReadBuffer.get(pipelineID);
    }

    public void free(final int[] offsets) {
        while (!atomic.compareAndSet(false, true))
            ;
        boolean resetWriteLimit = false;
        for (int offset : offsets) {
            int index = offset / tupleSize;
            bitmask[index] -= 1;
            if (index == 0 && bitmask[index] == 0 && writeBuffer.position() == writeBuffer.capacity()) {
                resetWriteLimit = true;
            }
            int modLimit = limit;
            if (modLimit == bitmask.length) {
                modLimit = 0;
            }
            if (modLimit == index && bitmask[index] == 0) {
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
                } while (bitmask[index] == 0);
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
        if (resetWriteLimit) {
            resetWriteLimit();
        }
        atomic.set(false);
    }

    public void resetReadLimit(String pipelineID) {
        while (!atomic.compareAndSet(false, true))
            ;
        // TODO: Handle edge case that would lead to a dead-lock here
        if (writeBuffer.position() != writeBuffer.capacity()) {
            ReadBuffer readBuffer = childPipelineToReadBuffer.get(pipelineID);
            readBuffer.getBuffer().position(0);
            readBuffer.mark();
            readBuffer.limit(writeBuffer.position());
        }
        atomic.set(false);
    }

    public void resetWriteLimit() {
        if (limit != bitmask.length) {
            writeBuffer.position(0);
            writeBuffer.limit(getLimitInBytes());
        } else if (bitmask[0] == 0) {
            writeBuffer.position(0);
            writeBuffer.limit(writeBuffer.capacity());
        }
    }

    private int getLimitInBytes() {
        return limit * tupleSize;
    }

    public ReadBuffer addReadBuffer(String pipelineID) {
        while (!atomic.compareAndSet(false, true))
            ;
        final ReadBuffer readBuffer = new ReadBuffer(writeBuffer.asReadOnlyBuffer(), writeBuffer.position());
        numberActiveReader++;
        atomic.set(false);
        childPipelineToReadBuffer.put(pipelineID, readBuffer);
        return readBuffer;
    }

    public void acquireAtomic() {
        while (!atomic.compareAndSet(false, true))
            ;
    }

    public void releaseAtomic() {
        atomic.set(false);
    }

    public void delete() {
        // TODO clean up memory for direct buffer
    }

    public boolean deregisterPipeline(final String childID) {
        boolean cleanUp;
        while (!atomic.compareAndSet(false, true))
            ;
        numberActiveReader--;
        childPipelineToReadBuffer.remove(childID).setFreezePosition(writeBuffer.position());
        cleanUp = numberActiveReader != 0;
        atomic.set(false);
        return cleanUp;
    }
}