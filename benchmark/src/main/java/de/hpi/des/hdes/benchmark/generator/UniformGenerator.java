package de.hpi.des.hdes.benchmark.generator;

import de.hpi.des.hdes.benchmark.BlockingOffer;
import de.hpi.des.hdes.benchmark.BlockingSocket;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
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
    long timeNow = startTime;
    final long finalEndTime = startTime + TimeUnit.SECONDS.toNanos(timeInSeconds);
    final var task = new Timer("writeFile");
    task.schedule(new TimerTask() {
      @Override
      public void run() {
        ((BlockingSocket) blockingSource).writeFile();
      }
    }, TimeUnit.SECONDS.toMillis(timeInSeconds + 1));

    while (totalSentEvents < totalEvents && timeNow <= finalEndTime) {
      timeNow = System.nanoTime();
      final long nanoDifference = timeNow - startTime;

      final long currentEventTarget = (long) (nanoDifference * this.eventsPerSecond / 1.0e9);
      final long missingEvents = currentEventTarget - totalSentEvents;

      // Ensures that we don't sent too many events
      final long eventsToBeSent = Math.min(totalEvents - totalSentEvents, missingEvents);

      // Send the events
      for (int i = 0; i < eventsToBeSent; i++) {
        try {
          blockingSource.offer(this.generateEvent());
          totalSentEvents += 1;
        } catch (IllegalStateException e) {
          log.info("Events remaining {} Events sent {}", totalEvents - totalSentEvents,
              totalSentEvents);
          this.shutdown();
          return false;
        }
        log.trace("Events to be sent {}", eventsToBeSent);
        if (i % 10_000 == 0 && System.nanoTime() > finalEndTime) {
          this.shutdown();
          return false; // To check if we have exceeded our max time
        }
      }
    }

    log.printf(Level.INFO, "Finished Events left %,d, Events sent %,d",
        totalEvents - totalSentEvents,
        totalSentEvents);
    //task.purge();
    this.shutdown();
    blockingSource.flush();
    return true;
  }

}
