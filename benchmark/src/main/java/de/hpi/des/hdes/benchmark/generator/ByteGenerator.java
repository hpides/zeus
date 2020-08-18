package de.hpi.des.hdes.benchmark.generator;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Sets;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ByteGenerator extends UniformGenerator<byte[]> {

    // Defines the size of the hash map during a join
    private static int VALUE_RANGE = 99_900;
    // JOIN_KEY_COUNT/(VALUE_RANGE + JOIN_KEY_COUNT) = desired selectivity
    private static int JOIN_KEY_COUNT = 100;

    @Getter
    private final List<Integer> values;
    private int i = 0;
    private long watermarkInterval = 100;
    private long lastWatermark = 0;
    private ByteBuffer buffer = ByteBuffer.allocate(17);

    public ByteGenerator(long eventsPerSecond, long timeInSeconds, ExecutorService executor) {
        this(eventsPerSecond, timeInSeconds, executor, 1);
    }

    public ByteGenerator(long eventsPerSecond, long timeInSeconds, ExecutorService executor, int seed) {
        super(eventsPerSecond, timeInSeconds, executor);

        values = IntStream.generate(new IntSupplier() {
            int i = seed * VALUE_RANGE;

            @Override
            public int getAsInt() {
                return this.i++;
            }
        }).limit(VALUE_RANGE).boxed().collect(Collectors.toList());
        for (int i = 1; i <= JOIN_KEY_COUNT; i++) {
            values.add(i);
        }
    }

    @Override
    protected byte[] generateEvent() {
        int value = values.get(i++ % values.size());
        long time = System.currentTimeMillis();
        byte watermark = 0;
        if (time - lastWatermark > watermarkInterval) {
            lastWatermark = time;
            watermark = 1;
            time -= 500;
        }
        buffer.position(0);
        // TODO: Make events larger and use different types for each side (e.g. nexmark)
        buffer.putLong(time).putInt(value).putInt(value * -1).put(watermark);

        return buffer.array();
    }

    public long expectedJoinSize(ByteGenerator other, long eps, long totalTime, int windowTime) {
        return (long) (Math
                .pow(Sets.intersection(Sets.newHashSet(this.values), Sets.newHashSet(other.getValues())).size()
                        * (eps / (double) values.size()), 2)
                * ((long) (totalTime - 10) / (double) windowTime)); // Data generator should run 10 seconds longer than
                                                                    // engine
    }

    public long expectedAggregationSize(long totalTime, int windowTime) {
        return (totalTime - 10) / windowTime;
    }
}