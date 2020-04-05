package de.hpi.des.hdes.benchmark.generator;

import de.hpi.des.hdes.benchmark.nexmark.NexmarkLightDataGenerator;
import java.util.concurrent.ExecutorService;
import org.jooq.lambda.tuple.Tuple5;

public class NexmarkLightAuctionGenerator extends UniformGenerator<Tuple5<Long, Integer, Integer, Integer, Long>> {
  NexmarkLightDataGenerator generator;

  public NexmarkLightAuctionGenerator(long eventsPerSecond, long timeInSeconds, ExecutorService executor, NexmarkLightDataGenerator generator) {
    super(eventsPerSecond, timeInSeconds, executor);
    this.generator = generator;
  }

  @Override
  protected Tuple5<Long, Integer, Integer, Integer, Long> generateEvent() {
    return generator.generateAuction();
  }
}
