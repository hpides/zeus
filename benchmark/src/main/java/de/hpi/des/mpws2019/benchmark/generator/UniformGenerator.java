package de.hpi.des.mpws2019.benchmark.generator;

import de.hpi.des.mpws2019.benchmark.TupleEvent;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UniformGenerator implements Generator<TupleEvent> {

  private final int eventsPerSecond;
  private final int timeInSeconds;
  private final ExecutorService executor;
  private final Random random = new Random();

  private long lastKey = 0;

  private TupleEvent generateRandomIntTuple() {
    final TupleEvent event = new TupleEvent(this.lastKey, this.random.nextInt(10000));
    this.lastKey++;
    return event;
  }

  public CompletableFuture<Boolean> generate(final Queue<TupleEvent> queue) {
    return CompletableFuture.supplyAsync(() -> this.sendEventsTimeAware(queue), executor);
  }

  @Override
  public Long getTotalEvents() {
    return Integer.toUnsignedLong(eventsPerSecond * timeInSeconds);
  }

  private Boolean sendEventsTimeAware(final Queue<TupleEvent> queue) {
    long sentEvents = 0;
    final int totalEvents = eventsPerSecond * timeInSeconds;
    final long startTime = System.nanoTime();

    while (sentEvents < totalEvents) {
      final long timeNow = System.nanoTime();
      final long nanoDifference = timeNow - startTime;

      final long currentEventTarget = (long) (nanoDifference * this.eventsPerSecond / 1.0e9);
      final long missingEvents = currentEventTarget - sentEvents;

      // Ensures that we don't sent too many events
      final long eventsToBeSent = Math.min(totalEvents - sentEvents, missingEvents);

      // Send the events
      LongStream.range(0, eventsToBeSent).forEach(i -> {
        queue.add(this.generateRandomIntTuple());
      });

      log.debug("Sent events: {}", sentEvents);
      log.debug("Missing events: {}", missingEvents);
      sentEvents += eventsToBeSent;
    }

    log.info("Finished generating events.");
    return true;
  }

}
