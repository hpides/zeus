package de.hpi.des.mpws2019.benchmark;

import static org.jooq.lambda.Seq.seq;

import de.hpi.des.mpws2019.engine.Engine;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class Benchmark {
  private final DataGenerator dataGenerator;
  private final Engine engine;
  private final TimedConcurrentBlockingQueue timedSource;
  private final TimedConcurrentBlockingQueue timedSink;

  public void run() {
    final CompletableFuture<Long> isFinished = dataGenerator.generate();
    engine.start();

    isFinished.thenRun(() -> {
      final Map<Long, Long> keyToEventTimeLatency = this.calculateEventTimeLatencies(
          timedSource.getKeyToEventTime(),
          timedSink.getKeyToEventTime()
      );
      printEventTimeMetrics(keyToEventTimeLatency);
    }).thenRun(engine::shutdown);
  }

  /**
   * Takes the timestamps from the timestamped queue and calculates the latency for each event.
   *
   * @param sourceTimestamps
   * @param sinkTimestamps
   * @return Queue that contains key -> eventTimeLatency timestamps.
   */
  public Map<Long, Long> calculateEventTimeLatencies(
      final Map<Long, Long> sourceTimestamps,
      final Map<Long, Long> sinkTimestamps
  ) {
    final HashMap<Long, Long> keyToEventTimeLatency = new HashMap<>();

    for (final long key : sourceTimestamps.keySet()) {
      final long sourceTimestamp = sourceTimestamps.get(key);
      final long sinkTimestamp = sinkTimestamps.get(key);
      final long latency = sinkTimestamp - sourceTimestamp;

      keyToEventTimeLatency.put(key, latency);
    }

    return keyToEventTimeLatency;
  }

  public void printEventTimeMetrics(Map<Long, Long> keyToEventTimeLatency) {
    final List<Long> eventTimeLatencies = seq(keyToEventTimeLatency).map(t -> t.v2).toList();
    final long totalLatency = seq(eventTimeLatencies).sum().get();
    final long totalEvents = seq(eventTimeLatencies).count();
    final long maxEventTimeLatency = seq(eventTimeLatencies).max().get();
    final long minEventTimeLatency = seq(eventTimeLatencies).min().get();

    System.out.println("Total events: " + totalEvents);
    System.out.println("Average latency: " + totalLatency / totalEvents + "ns");
    System.out.println("Max. latency: " + maxEventTimeLatency + "ns");
    System.out.println("Min. latency: " + minEventTimeLatency + "ns");
  }

}
