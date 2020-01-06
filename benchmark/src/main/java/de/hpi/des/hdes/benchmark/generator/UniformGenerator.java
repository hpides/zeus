package de.hpi.des.hdes.benchmark.generator;

import de.hpi.des.hdes.benchmark.TimedBlockingSource;
import de.hpi.des.hdes.benchmark.TupleEvent;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UniformGenerator implements Generator<TupleEvent> {

  private final int eventsPerSecond;
  private final int timeInSeconds;
  private final ExecutorService executor;
  private final Random random = new Random();
  @Getter
  private final int benchmarkCheckpointInterval = 10000;

  private long lastKey = 0;

  private TupleEvent generateRandomIntTuple(boolean isBenchmarkCheckpoint) {
    final TupleEvent event = new TupleEvent(this.lastKey, this.random.nextInt(10000), isBenchmarkCheckpoint);
    this.lastKey++;
    return event;
  }

  public CompletableFuture<Boolean> generate(final TimedBlockingSource<TupleEvent> timedBlockingSource) {
    return CompletableFuture.supplyAsync(() -> this.sendEventsTimeAware(timedBlockingSource), executor);
  }

  @Override
  public Long getTotalEvents() {
    return Integer.toUnsignedLong(eventsPerSecond) * Integer.toUnsignedLong(timeInSeconds);
  }

  @Override
  public void shutdown() {
    executor.shutdownNow();
  }

  private Boolean sendEventsTimeAware(final TimedBlockingSource<TupleEvent> timedBlockingSource) {
    long sentEvents = 0;
    this.lastKey = 0;
    final int totalEvents = eventsPerSecond * timeInSeconds;
    final long startTime = System.nanoTime();
    long benchmarkCheckpointCounter = 0;

    while (sentEvents < totalEvents) {
      final long timeNow = System.nanoTime();
      final long nanoDifference = timeNow - startTime;

      final long currentEventTarget = (long) (nanoDifference * this.eventsPerSecond / 1.0e9);
      final long missingEvents = currentEventTarget - sentEvents;

      // Ensures that we don't sent too many events
      final long eventsToBeSent = Math.min(totalEvents - sentEvents, missingEvents);

      // Send the events
      for(int i = 0; i < eventsToBeSent; i++) {
        if(benchmarkCheckpointCounter % benchmarkCheckpointInterval == 0) {
          timedBlockingSource.offer(this.generateRandomIntTuple(true));
          log.trace("Events to be sent {}", eventsToBeSent);
          log.trace("Current Queue Size {}", timedBlockingSource.getQueue().size());
        }
        else {
          timedBlockingSource.offer(this.generateRandomIntTuple(false));
        }
        benchmarkCheckpointCounter++;
      }

      log.debug("Sent events: {}", sentEvents);
      log.debug("Missing events: {}", missingEvents);
      sentEvents += eventsToBeSent;
    }

    log.info("Finished generating events.");
    return true;
  }

}
