package de.hpi.des.hdes.benchmark.generator;

import de.hpi.des.hdes.benchmark.BlockingSource;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;

@Log4j2
@RequiredArgsConstructor
public abstract class UniformGenerator<E> implements Generator<E> {

  private final int eventsPerSecond;
  private final int timeInSeconds;
  private final ExecutorService executor;

  @Getter
  private final int benchmarkCheckpointInterval = 10000;


  protected abstract E generateEvent(boolean isBenchmark);

  public CompletableFuture<Boolean> generate(final BlockingSource<E> blockingSource) {
    return CompletableFuture.supplyAsync(() -> this.sendEventsTimeAware(blockingSource), executor);
  }

  @Override
  public Long getTotalEvents() {
    return Integer.toUnsignedLong(eventsPerSecond) * Integer.toUnsignedLong(timeInSeconds);
  }

  @Override
  public void shutdown() {
    executor.shutdownNow();
  }

  private Boolean sendEventsTimeAware(final BlockingSource<E> blockingSource) {
    long sentEvents = 0;
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
      for (int i = 0; i < eventsToBeSent; i++) {
        if (benchmarkCheckpointCounter % benchmarkCheckpointInterval == 0) {
          blockingSource.offer(this.generateEvent(true));
//          log.trace("Events to be sent {}", eventsToBeSent);
//          log.trace("Current Queue Size {}", blockingSource.getQueue().size());
        } else {
          blockingSource.offer(this.generateEvent(false));
        }
        benchmarkCheckpointCounter++;
      }

//      log.trace("Sent events: {}", sentEvents);
//      log.trace("Missing events: {}", missingEvents);
      sentEvents += eventsToBeSent;
      if (eventsToBeSent == 1) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    log.printf(Level.INFO, "Finished generating events. Sent %,d events", sentEvents);
    blockingSource.getQueue().flush();
    this.shutdown();
    return true;
  }

}
