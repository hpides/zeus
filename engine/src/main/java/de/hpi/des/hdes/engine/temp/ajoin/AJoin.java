package de.hpi.des.hdes.engine.temp.ajoin;

import java.nio.ByteBuffer;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import de.hpi.des.hdes.engine.execution.buffer.ReadBuffer;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntArraySet;

public class AJoin implements Runnable {
    final private long windowSlide;
    final private long windowLength;
    private long earliestOpenWindowJoin = Long.MAX_VALUE;
    private long latestWatermarkLeft;
    private long latestWatermarkRight;
    final private int leftTupleLength = 8;
    final private int rightTupleLength = 8;
    final private int leftKeyOffset = 0;
    final private int rightKeyOffset = 0;
    final private ReadBuffer leftInput;
    final private ReadBuffer rightInput;
    final private ByteBuffer leftByteBuffer;
    final private ByteBuffer rightByteBuffer;
    final private Dispatcher dispatcher;
    final String pipelineID;

    final private Long2ObjectMap<Int2ObjectOpenHashMap<IntSet>> leftWindowsToBuckets = new Long2ObjectOpenHashMap<>();
    final private Long2ObjectMap<Int2ObjectOpenHashMap<IntSet>> rightWindowsToBuckets = new Long2ObjectOpenHashMap<>();
    // TEMP
    private long count = 1;

    public AJoin(final ReadBuffer left, final ReadBuffer right, final Dispatcher dispatcher, final long windowLength,
            final long windowSlide, final String pipelineID) {
        leftInput = left;
        rightInput = right;
        leftByteBuffer = left.getBuffer();
        rightByteBuffer = right.getBuffer();
        this.dispatcher = dispatcher;
        this.windowLength = windowLength;
        this.windowSlide = windowSlide;
        this.pipelineID = pipelineID;
    }

    public void readEventLeft() {
        final long eventTimestamp = leftByteBuffer.getLong();
        // Marks begining of values
        leftByteBuffer.mark();
        if (eventTimestamp < latestWatermarkLeft) {
            leftByteBuffer.position(leftByteBuffer.position() + leftTupleLength + 1);
            return;
        }
        final long windowStart = eventTimestamp - (eventTimestamp % windowLength);
        if (earliestOpenWindowJoin > windowStart) {
            earliestOpenWindowJoin = windowStart;
        }

        // {{{operatorImplementation}}}
        Int2ObjectOpenHashMap<IntSet> bucket = leftWindowsToBuckets.computeIfAbsent(windowStart,
                w -> new Int2ObjectOpenHashMap<IntSet>());
        leftByteBuffer.position(leftByteBuffer.position() + leftKeyOffset);
        IntSet set = bucket.computeIfAbsent(leftByteBuffer.getInt(), w -> new IntArraySet());
        leftByteBuffer.reset();
        // Custom offset needed for key?s
        set.add(leftByteBuffer.position());
        leftByteBuffer.position(leftByteBuffer.position() + leftTupleLength);

        if (leftByteBuffer.get() == (byte) 1) {
            latestWatermarkLeft = Math.max(latestWatermarkLeft, eventTimestamp);
            for (; earliestOpenWindowJoin + windowLength <= latestWatermarkLeft && earliestOpenWindowJoin
                    + windowLength <= latestWatermarkRight; earliestOpenWindowJoin += windowSlide) {
                trigger(earliestOpenWindowJoin);
            }
        }
    }

    public void readEventRight() {
        final long eventTimestamp = rightByteBuffer.getLong();
        // Marks beginning of values
        rightByteBuffer.mark();
        if (eventTimestamp < latestWatermarkRight) {
            rightByteBuffer.position(rightByteBuffer.position() + rightTupleLength + 1);
            return;
        }
        final long windowStart = eventTimestamp - (eventTimestamp % windowLength);
        if (earliestOpenWindowJoin > windowStart) {
            earliestOpenWindowJoin = windowStart;
        }
        // {{{operatorImplementation}}}
        Int2ObjectOpenHashMap<IntSet> bucket = rightWindowsToBuckets.computeIfAbsent(windowStart,
                w -> new Int2ObjectOpenHashMap<IntSet>());
        rightByteBuffer.position(rightByteBuffer.position() + rightKeyOffset);
        IntSet set = bucket.computeIfAbsent(rightByteBuffer.getInt(), w -> new IntArraySet());
        rightByteBuffer.reset();
        // Custom offset needed for key?s
        set.add(rightByteBuffer.position());
        rightByteBuffer.position(rightByteBuffer.position() + rightTupleLength);

        if (rightByteBuffer.get() == (byte) 1) {
            latestWatermarkRight = Math.max(latestWatermarkRight, eventTimestamp);
            for (; earliestOpenWindowJoin + windowLength <= latestWatermarkLeft && earliestOpenWindowJoin
                    + windowLength <= latestWatermarkRight; earliestOpenWindowJoin += windowSlide) {
                trigger(earliestOpenWindowJoin);
            }
        }
    }

    private void trigger(final long timeStamp) {
        Int2ObjectOpenHashMap<IntSet> leftBucket = leftWindowsToBuckets.get(timeStamp);
        Int2ObjectOpenHashMap<IntSet> rightBucket = rightWindowsToBuckets.get(timeStamp);
        int maxLeftOffset = 0;
        int maxRightOffset = 0;
        for (int key : leftBucket.keySet()) {
            if (rightBucket.containsKey(key)) {
                for (int leftOffset : leftBucket.get(key)) {
                    for (int rightOffset : rightBucket.get(key)) {
                        this.writeOutput(leftOffset, rightOffset);
                        maxLeftOffset = Math.max(leftOffset, maxLeftOffset);
                        maxRightOffset = Math.max(rightOffset, maxRightOffset);
                    }
                }
                ;
            }
        }
    }

    public void writeOutput(int leftOffset, int rightOffset) {
        leftByteBuffer.position(leftOffset);
        rightByteBuffer.position(rightOffset);
        byte[] out = new byte[8 + 1 + leftTupleLength + rightTupleLength];
        ByteBuffer b = ByteBuffer.wrap(out).putLong(count++);
        leftByteBuffer.get(out, 8, leftTupleLength);
        rightByteBuffer.get(out, 8 + leftTupleLength, rightTupleLength);
        b.put(8 + leftTupleLength + rightTupleLength, (byte) 0);
        while (!dispatcher.write(pipelineID, out))
            ;
        dispatcher.free(leftInput, leftOffset - 8);
        dispatcher.free(rightInput, rightOffset - 8);
        // This does not erase the data, but set position = 0, limit = capacity and
        // discards mark.
        leftByteBuffer.clear();
        rightByteBuffer.clear();
        return;
    }

    @Override
    public void run() {
        while (true) {
            if (leftByteBuffer.hasRemaining())
                readEventLeft();
            if (rightByteBuffer.hasRemaining())
                readEventRight();
            if (leftByteBuffer.position() == leftByteBuffer.capacity()) {
                dispatcher.resetLimit(leftInput);
            }
            if (rightByteBuffer.position() == rightByteBuffer.capacity()) {
                dispatcher.resetLimit(rightInput);
            }
        }
    }
}
