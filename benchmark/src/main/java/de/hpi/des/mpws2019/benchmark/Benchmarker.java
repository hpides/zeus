package de.hpi.des.mpws2019.benchmark;

import static org.jooq.lambda.Seq.seq;

import de.hpi.des.mpws2019.engine.Engine;
import de.hpi.des.mpws2019.engine.sink.QueueSink;
import de.hpi.des.mpws2019.engine.source.QueueSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;

public class Benchmarker {

  public void run() throws InterruptedException, ExecutionException {

    final var eventsPerSecond = 50000;
    final var numberOfEvents = 20 * eventsPerSecond;

    final TimedConcurrentBlockingQueue<TupleEvent> source = new TimedConcurrentBlockingQueue<>(
        1 * eventsPerSecond,
        numberOfEvents
    );
    final TimedConcurrentBlockingQueue<TupleEvent> sink = new TimedConcurrentBlockingQueue<>(
        numberOfEvents);

    final DataGenerator dataGenerator = new DataGenerator(
        eventsPerSecond,
        numberOfEvents,
        source
    );

    final Future<Long> isFinished = dataGenerator.generateDataTimeAware();

    final QueueSource<TupleEvent> queueSource = new QueueSource<>(source);
    final QueueSink<TupleEvent> queueSink = new QueueSink<>(sink);

    final Function<TupleEvent, TupleEvent> mapping =
        event -> new TupleEvent(event.getKey(), event.getValue() + 1);
    final Engine<TupleEvent> engine = new Engine<>(queueSource, queueSink, mapping);

    engine.start();

    System.out.println(isFinished.get());

    engine.shutdown();

    final Map<Long, Long> keyToEventTimeLatency =
        this.calculateEventTimeLatencies(source.getKeyToEventTime(), sink.getKeyToEventTime());

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


}
