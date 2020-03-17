package de.hpi.des.hdes.benchmark.generator;

import de.hpi.des.hdes.benchmark.BlockingOffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;

@Log4j2
@RequiredArgsConstructor
public abstract class UniformGenerator<E> implements Generator<E> {

  private final long eventsPerSecond;
  private final long timeInSeconds;
  private final ExecutorService executor;

  @Getter
  private final int benchmarkCheckpointInterval = 10000;


  protected abstract E generateEvent();

  public CompletableFuture<Boolean> generate(final BlockingOffer<E> blockingSource) {
    return CompletableFuture.supplyAsync(() -> this.sendEventsTimeAware(blockingSource), executor);
  }

  @Override
  public Long getTotalEvents() {
    return eventsPerSecond * timeInSeconds;
  }

  @Override
  public void shutdown() {
    executor.shutdownNow();
  }

  private Boolean sendEventsTimeAware(final BlockingOffer<E> blockingSource) {
    long totalSentEvents = 0;
    final long totalEvents = eventsPerSecond * timeInSeconds;
    final long startTime = System.nanoTime();

    while (totalSentEvents < totalEvents) {
      final long timeNow = System.nanoTime();
      final long nanoDifference = timeNow - startTime;

      final long currentEventTarget = (long) (nanoDifference * this.eventsPerSecond / 1.0e9);
      final long missingEvents = currentEventTarget - totalSentEvents;

      // Ensures that we don't sent too many events
      final long eventsToBeSent = Math.min(totalEvents - totalSentEvents, missingEvents);

      // Send the events
      for (int i = 0; i < eventsToBeSent; i++) {
        try {
          blockingSource.offer(this.generateEvent());
          totalSentEvents +=1;
        } catch (IllegalStateException e) {
          log.printf(Level.WARN, "Events left %,d, Events sent %,d", totalEvents - totalSentEvents,
              totalSentEvents,
              e);
          this.shutdown();
          return false;
        }
        log.trace("Events to be sent {}", eventsToBeSent);
      }
    }

    log.printf(Level.INFO, "Finished generating events. Sent %,d events", totalSentEvents);
    blockingSource.flush();
    this.shutdown();
    return true;
  }

}
