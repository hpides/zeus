package de.hpi.des.mpws2019.benchmark;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.LongStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DataGenerator {

  private final int eventsPerSecond;
  private final int numberOfEvents;
  private final Queue<TupleEvent> queue;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final Random random = new Random();
  private long lastKey = 0;

  private TupleEvent generateRandomIntTuple() {
    final TupleEvent event = new TupleEvent(this.lastKey, this.random.nextInt(10000));
    this.lastKey++;
    return event;
  }

  public Future<Long> generateDataTimeAware() {
    return this.executor.submit(this::sendEventsTimeAware);
  }

  private Long sendEventsTimeAware() {
    long sentEvents = 0;
    final long startTime = System.nanoTime();

    while (sentEvents < this.numberOfEvents) {
      final long timeNow = System.nanoTime();
      final long nanoDifference = timeNow - startTime;

      final long currentEventTarget = (long) (nanoDifference * this.eventsPerSecond / 1.0e9);
      final long missingEvents = currentEventTarget - sentEvents;

      // Ensures that we don't sent too many events
      final long eventsToBeSent = Math.min(this.numberOfEvents - sentEvents, missingEvents);

      // Send the events
      LongStream.range(0, eventsToBeSent).forEach(i -> {
        this.queue.add(this.generateRandomIntTuple());
      });

      log.info("Sent events: {}", sentEvents);
      sentEvents += eventsToBeSent;
    }

    log.info("Finished generating events.");
    this.executor.shutdown();
    return sentEvents;
  }

}
