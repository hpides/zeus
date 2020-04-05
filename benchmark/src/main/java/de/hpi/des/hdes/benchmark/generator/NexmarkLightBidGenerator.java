package de.hpi.des.hdes.benchmark.generator;

import de.hpi.des.hdes.benchmark.nexmark.NexmarkLightDataGenerator;
import java.util.concurrent.ExecutorService;
import org.jooq.lambda.tuple.Tuple5;

public class NexmarkLightBidGenerator extends UniformGenerator<Tuple5<Long, Long, Integer, Integer, Long>> {
  NexmarkLightDataGenerator generator;

  public NexmarkLightBidGenerator(long eventsPerSecond, long timeInSeconds, ExecutorService executor, NexmarkLightDataGenerator generator) {
    super(eventsPerSecond, timeInSeconds, executor);
    this.generator = generator;
  }

  @Override
  protected Tuple5<Long, Long, Integer, Integer, Long> generateEvent() {
    return generator.generateBid();
  }
}
