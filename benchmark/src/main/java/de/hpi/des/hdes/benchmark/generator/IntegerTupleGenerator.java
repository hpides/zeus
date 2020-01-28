package de.hpi.des.hdes.benchmark.generator;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import org.jooq.lambda.tuple.Tuple1;

public class IntegerTupleGenerator extends UniformGenerator<Tuple1<Integer>> {

  private final Random random = new Random();

  public IntegerTupleGenerator(int eventsPerSecond, int timeInSeconds, ExecutorService executor) {
    super(eventsPerSecond, timeInSeconds, executor);
  }

  @Override
  protected Tuple1<Integer> generateEvent(boolean isBenchmark) {
    return new Tuple1<>(this.random.nextInt(10000));
  }
}
