package de.hpi.des.hdes.benchmark.generator;

import java.util.concurrent.ExecutorService;
import org.jooq.lambda.tuple.Tuple2;

public class NexmarkLightGenerator extends UniformGenerator<Tuple2<Integer, Long>> {

  public NexmarkLightGenerator(long eventsPerSecond, long timeInSeconds, ExecutorService executor) {
    super(eventsPerSecond, timeInSeconds, executor);
  }

  @Override
  protected Tuple2<Integer, Long> generateEvent() {
    return null;
  }
}
