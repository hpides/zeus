package de.hpi.des.mpws2019.benchmark;

import static org.jooq.lambda.Seq.seq;

import de.hpi.des.mpws2019.benchmark.generator.Generator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Getter
@RequiredArgsConstructor
@Slf4j
public class Metrics {
    private final Generator usedGenerator;

    private final long totalEvents;

    private final double averageEventTimeLatency;
    private final double maxEventTimeLatency;
    private final double minEventTimeLatency;

    private final double averageProcessingTimeLatency;
    private final double maxProcessingTimeLatency;
    private final double minProcessingTimeLatency;

    public static Metrics from(Generator usedGenerator, TimedBlockingSource timedSource, TimedBlockingSink timedSink) {
        log.info("Processing Benchmark Results");
        final Map<Long, Long> keyToEventTimeLatency = calculateLatencies(
                timedSource.getBenchmarkCheckpointToAddTime(),
                timedSink.getBenchmarkCheckpointToAddTime()
        );

        final Map<Long, Long> keyToProcessingTimeLatency = calculateLatencies(
                timedSource.getBenchmarkCheckpointToRemoveTime(),
                timedSink.getBenchmarkCheckpointToAddTime()
        );

        final List<Long> eventTimeLatencies = seq(keyToEventTimeLatency).map(t -> t.v2).toList();
        final List<Long> processingTimeLatencies = seq(keyToProcessingTimeLatency).map(t -> t.v2).toList();
        final double totalEventTimeLatency = transformNStoMS(seq(eventTimeLatencies).sum().get());
        final double totalProcessingLatency = transformNStoMS(seq(processingTimeLatencies).sum().get());

        final long totalEvents = timedSink.getSinkSize();

        final double averageEventTimeLatency = totalEventTimeLatency / totalEvents;
        final double maxEventTimeLatency = transformNStoMS(seq(eventTimeLatencies).max().get());
        final double minEventTimeLatency = transformNStoMS(seq(eventTimeLatencies).min().get());

        final double averageProcessingTimeLatency = totalProcessingLatency / totalEvents;
        final double maxProcessingTimeLatency = transformNStoMS(seq(processingTimeLatencies).max().get());
        final double minProcessingTimeLatency = transformNStoMS(seq(processingTimeLatencies).min().get());

        return new Metrics(usedGenerator,
                totalEvents,
                averageEventTimeLatency,
                maxEventTimeLatency,
                minEventTimeLatency,
                averageProcessingTimeLatency,
                maxProcessingTimeLatency,
                minProcessingTimeLatency);
    }

    /**
     * Takes the timestamps from the timestamped queue and calculates the latency for each event.
     *
     * @return Queue that contains key -> eventTimeLatency timestamps.
     */
    private static Map<Long, Long> calculateLatencies(
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

    private static double transformNStoMS(long nanoseconds) {
        return nanoseconds / 1e9;
    }

    public void print() {
        System.out.println("---- General Metrics ----");
        System.out.format("Processed Events: %,d%n", this.getTotalEvents());
        System.out.println();
        System.out.println("---- Event Time Latency ----");
        System.out.println("Avg: " + this.getAverageEventTimeLatency() + " ms");
        System.out.println("Max: " + this.getMaxEventTimeLatency() + " ms");
        System.out.println("Min: " + this.getMinEventTimeLatency() + " ms");
        System.out.println();
        System.out.println("---- Processing Time Latency ----");
        System.out.println("Avg: " + this.getAverageProcessingTimeLatency() + " ms");
        System.out.println("Max: " + this.getMaxProcessingTimeLatency() + " ms");
        System.out.println("Min: " + this.getMinProcessingTimeLatency() + " ms");
        System.out.println();
    }
}
