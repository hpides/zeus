package de.hpi.des.hdes.benchmark.generator;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.Getter;

public class ByteGenerator extends UniformGenerator<byte[]> {

    private final Random random;
    @Getter
    private final List<Integer> values;
    private int i = 0;
    private long watermarkInterval = 100;
    private long lastWatermark = 0;
    private static ByteBuffer buffer = ByteBuffer.allocate(17);

    public ByteGenerator(long eventsPerSecond, long timeInSeconds, ExecutorService executor) {
        this(eventsPerSecond, timeInSeconds, executor, 1);
    }

    public ByteGenerator(long eventsPerSecond, long timeInSeconds, ExecutorService executor, int seed) {
        super(eventsPerSecond, timeInSeconds, executor);
        this.random = new Random(seed);

        values = IntStream.generate(new IntSupplier() {
            int i = seed * 10_000;

            @Override
            public int getAsInt() {
                return this.i++;
            }
        }).limit(10_000).boxed().collect(Collectors.toList());
        values.add(1);
    }

    @Override
    protected byte[] generateEvent() {
        int value = values.get(i++ % values.size());
        long time = System.currentTimeMillis();
        buffer.putLong(0, time).putInt(value).putInt(value * -1)
                .put((time - lastWatermark > watermarkInterval) ? (byte) 1 : (byte) 0);

        return buffer.array();
    }

}