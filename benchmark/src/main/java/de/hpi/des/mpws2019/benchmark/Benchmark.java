package de.hpi.des.mpws2019.benchmark;

import static org.jooq.lambda.Seq.seq;

import de.hpi.des.mpws2019.benchmark.generator.Generator;
import de.hpi.des.mpws2019.engine.Engine;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class Benchmark {
    private final DataGenerator dataGenerator;
    private final Engine engine;
    private final TimedConcurrentBlockingQueue timedSource;
    private final TimedConcurrentBlockingQueue timedSink;

    public void run() {
        engine.start();
        final CompletableFuture<Long> isFinished = dataGenerator.generate();

        try {
            isFinished.get();

            final Map<Long, Long> keyToEventTimeLatency = this.calculateLatencies(
                    timedSource.getKeyToAddTime(),
                    timedSink.getKeyToAddTime()
            );

            final Map<Long, Long> keyToProcessingTimeLatency = this.calculateLatencies(
                    timedSource.getKeyToRemoveTimeHashMap(),
                    timedSink.getKeyToAddTime()
            );

            System.out.println("---- Event Time Latency ----");
            printLatencyMetrics(keyToEventTimeLatency);
            System.out.println("---- Processing Time Latency ----");
            printLatencyMetrics(keyToProcessingTimeLatency);
        } catch (ExecutionException | InterruptedException e) {
            log.error(e.getMessage());
        } finally {
            engine.shutdown();
        }


    }

    /**
     * Takes the timestamps from the timestamped queue and calculates the latency for each event.
     *
     * @return Queue that contains key -> eventTimeLatency timestamps.
     */
    public Map<Long, Long> calculateLatencies(
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

    private double transformNStoMS(long nanoseconds) {
        return nanoseconds / 1e9;
    }

    public void printLatencyMetrics(Map<Long, Long> keyToEventTimeLatency) {
        final List<Long> eventTimeLatencies = seq(keyToEventTimeLatency).map(t -> t.v2).toList();
        final long totalEvents = seq(eventTimeLatencies).count();
        final double totalLatency = transformNStoMS(seq(eventTimeLatencies).sum().get());
        final double maxEventTimeLatency = transformNStoMS(seq(eventTimeLatencies).max().get());
        final double minEventTimeLatency = transformNStoMS(seq(eventTimeLatencies).min().get());

        System.out.println("Total events: " + totalEvents);
        System.out.println("Average latency: " + totalLatency / totalEvents + " ms");
        System.out.println("Max. latency: " + maxEventTimeLatency + " ms");
        System.out.println("Min. latency: " + minEventTimeLatency + " ms");
        System.out.println();
    }

}
