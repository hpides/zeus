package de.hpi.des.hdes.benchmark.generator;

import com.google.common.collect.Sets;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import org.jooq.lambda.tuple.Tuple1;

public class IntegerTupleGenerator extends UniformGenerator<Tuple1<Integer>> {

  private final Random random;
  @Getter
  private final List<Integer> values;
  private int i = 0;

  public IntegerTupleGenerator(int eventsPerSecond, int timeInSeconds, ExecutorService executor,
      int seed) {
    super(eventsPerSecond, timeInSeconds, executor);
    this.random = new Random(seed);
    values = IntStream.generate(() -> random.nextInt(1_000_000)).limit(1_000).boxed()
        .collect(Collectors.toList());
  }

  public int expectedJoinSize(IntegerTupleGenerator other, int eps) {
    return Sets.intersection(
        Sets.newHashSet(this.values), Sets.newHashSet(other.getValues())).size() * eps;
  }

  public IntegerTupleGenerator(int eventsPerSecond, int timeInSeconds, ExecutorService executor) {
    this(eventsPerSecond, timeInSeconds, executor, 1);
  }


  @Override
  protected Tuple1<Integer> generateEvent(boolean isBenchmark) {
    return new Tuple1<>(values.get(i++ % values.size()));
  }
}
