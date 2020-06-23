package de.hpi.des.hdes.engine.temp.ajoin;

import java.nio.ByteBuffer;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntArraySet;

public class AJoin implements Runnable {
    final private long windowSlide;
    final private long windowLength;
    private long earliestOpenWindowJoin = Long.MAX_VALUE;
    private long latestTimestampLeft;
    private long latestTimestampRight;
    final private int leftTupleLength = 8;
    final private int rightTupleLength = 8;
    final private ByteBuffer leftInput;
    final private ByteBuffer rightInput;
    final private Dispatcher dispatcher;

    final private Long2ObjectMap<Int2ObjectOpenHashMap<IntSet>> leftWindowsToBuckets = new Long2ObjectOpenHashMap<>();
    final private Long2ObjectMap<Int2ObjectOpenHashMap<IntSet>> rightWindowsToBuckets = new Long2ObjectOpenHashMap<>();
    // TEMP
    private long count = 1;

    public AJoin(final ByteBuffer left, final ByteBuffer right, final Dispatcher dispatcher, final long windowLength,
            final long windowSlide) {
        leftInput = left;
        rightInput = right;
        this.dispatcher = dispatcher;
        this.windowLength = windowLength;
        this.windowSlide = windowSlide;
    }

    public void readEventLeft() {
        final long eventTimestamp = leftInput.getLong();
        // Marks begining of values
        leftInput.mark();
        if (eventTimestamp < latestTimestampLeft) {
            return;
        }
        final long windowStart = eventTimestamp - (eventTimestamp % windowLength);
        if (earliestOpenWindowJoin > windowStart) {
            earliestOpenWindowJoin = windowStart;
        }

        Int2ObjectOpenHashMap<IntSet> bucket = leftWindowsToBuckets.computeIfAbsent(windowStart,
                w -> new Int2ObjectOpenHashMap<IntSet>());
        IntSet set = bucket.computeIfAbsent(leftInput.getInt(), w -> new IntArraySet());
        leftInput.reset();
        // Custom offset needed for key?s
        set.add(leftInput.position());
        leftInput.position(leftInput.position() + leftTupleLength);

        if (leftInput.getChar() == (char) 1) {
            latestTimestampLeft = Math.max(latestTimestampLeft, eventTimestamp);
            for (; earliestOpenWindowJoin + windowLength <= latestTimestampLeft && earliestOpenWindowJoin
                    + windowLength <= latestTimestampRight; earliestOpenWindowJoin += windowSlide) {
                trigger(earliestOpenWindowJoin);
            }
        }
    }

    public void readEventRight() {
        final long eventTimestamp = rightInput.getLong();
        // Marks beginning of values
        rightInput.mark();
        if (eventTimestamp < latestTimestampRight) {
            return;
        }
        final long windowStart = eventTimestamp - (eventTimestamp % windowLength);
        if (earliestOpenWindowJoin > windowStart) {
            earliestOpenWindowJoin = windowStart;
        }
        Int2ObjectOpenHashMap<IntSet> bucket = rightWindowsToBuckets.computeIfAbsent(windowStart,
                w -> new Int2ObjectOpenHashMap<IntSet>());
        IntSet set = bucket.computeIfAbsent(rightInput.getInt(), w -> new IntArraySet());
        rightInput.reset();
        // Custom offset needed for key?s
        set.add(rightInput.position());
        rightInput.position(rightInput.position() + rightTupleLength);

        if (rightInput.getChar() == (char) 1) {
            latestTimestampRight = Math.max(latestTimestampRight, eventTimestamp);
            for (; earliestOpenWindowJoin + windowLength <= latestTimestampLeft && earliestOpenWindowJoin
                    + windowLength <= latestTimestampRight; earliestOpenWindowJoin += windowSlide) {
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
        dispatcher.free(leftInput, maxLeftOffset);
        dispatcher.free(rightInput, maxRightOffset);
    }

    public void writeOutput(int leftOffset, int rightOffset) {
        leftInput.position(leftOffset);
        rightInput.position(rightOffset);
        byte[] out = new byte[8 + 2 + leftTupleLength + rightTupleLength];
        ByteBuffer b = ByteBuffer.wrap(out).putLong(count++);
        leftInput.get(out, 8, leftTupleLength);
        rightInput.get(out, 8 + leftTupleLength, rightTupleLength);
        b.putChar(8 + leftTupleLength + rightTupleLength, (char) 0);
        while (!dispatcher.write(this, out))
            ;
        // This does not erase the data, but set position = 0, limit = capacity and
        // discards mark.
        leftInput.clear();
        rightInput.clear();
        return;
    }

    @Override
    public void run() {
        while (true) {
            if (leftInput.hasRemaining())
                readEventLeft();
            if (rightInput.hasRemaining())
                readEventRight();
            if (leftInput.position() == leftInput.capacity()) {
                dispatcher.resetLimit(leftInput);
            }
            if (rightInput.position() == rightInput.capacity()) {
                dispatcher.resetLimit(rightInput);
            }
        }
    }
}
