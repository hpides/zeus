package de.hpi.des.hdes.benchmark.generator;

import com.google.common.collect.Sets;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import org.jooq.lambda.tuple.Tuple2;

public class IntegerTupleGenerator extends UniformGenerator<Tuple2<Integer, Long>> {

    private final Random random;
    @Getter
    private final List<Integer> values;
    private int i = 0;

    public IntegerTupleGenerator(long eventsPerSecond, long timeInSeconds, ExecutorService executor, int seed) {
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

    public long expectedJoinSize(IntegerTupleGenerator other, long eps, long totalTime, int windowTime) {
        return (long) (Math
                .pow(Sets.intersection(Sets.newHashSet(this.values), Sets.newHashSet(other.getValues())).size()
                        * (eps / (double) values.size()), 2)
                * ((totalTime - 10l) / (double) windowTime)); // -10 due to generators that are supposed to run 10
                                                              // seconds longer than engines.
    }

    public IntegerTupleGenerator(int eventsPerSecond, int timeInSeconds, ExecutorService executor) {
        this(eventsPerSecond, timeInSeconds, executor, 1);
    }

    @Override
    protected Tuple2<Integer, Long> generateEvent() {
        return new Tuple2<>(values.get(i++ % values.size()), System.currentTimeMillis());
    }

}
