package de.hpi.des.mpws2019.benchmark;

import static org.jooq.lambda.Seq.seq;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class MetricsManager {

    public MetricsResult evaluate(BenchmarkResult benchmarkResult) {
        log.info("Processing Benchmark Results");
        final Map<Long, Long> keyToEventTimeLatency = this.calculateLatencies(
                benchmarkResult.getTimedSource().getBenchmarkCheckpointToAddTime(),
                benchmarkResult.getTimedSink().getBenchmarkCheckpointToAddTime()
        );

        final Map<Long, Long> keyToProcessingTimeLatency = this.calculateLatencies(
                benchmarkResult.getTimedSource().getBenchmarkCheckpointToRemoveTime(),
                benchmarkResult.getTimedSink().getBenchmarkCheckpointToAddTime()
        );

        final List<Long> eventTimeLatencies = seq(keyToEventTimeLatency).map(t -> t.v2).toList();
        final List<Long> processingTimeLatencies = seq(keyToProcessingTimeLatency).map(t -> t.v2).toList();
        final double totalEventTimeLatency = transformNStoMS(seq(eventTimeLatencies).sum().get());
        final double totalProcessingLatency = transformNStoMS(seq(processingTimeLatencies).sum().get());

        MetricsResult metricsResult = new MetricsResult();
        metricsResult.setTotalEvents(benchmarkResult.getTimedSink().getSinkSize());

        metricsResult.setAverageEventTimeLatency(totalEventTimeLatency / metricsResult.getTotalEvents());
        metricsResult.setMaxEventTimeLatency(transformNStoMS(seq(eventTimeLatencies).max().get()));
        metricsResult.setMinEventTimeLatency(transformNStoMS(seq(eventTimeLatencies).min().get()));

        metricsResult.setAverageProcessingTimeLatency(totalProcessingLatency / metricsResult.getTotalEvents());
        metricsResult.setMaxProcessingTimeLatency(transformNStoMS(seq(processingTimeLatencies).max().get()));
        metricsResult.setMinProcessingTimeLatency(transformNStoMS(seq(processingTimeLatencies).min().get()));

        return metricsResult;
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

    public void printMetrics(MetricsResult metricsResult) {
        System.out.println("---- General Metrics ----");
        System.out.format("Processed Events: %,d%n", metricsResult.getTotalEvents());
        System.out.println();
        System.out.println("---- Event Time Latency ----");
        System.out.println("Avg: " + metricsResult.getAverageEventTimeLatency() + " ms");
        System.out.println("Max: " + metricsResult.getMaxEventTimeLatency() + " ms");
        System.out.println("Min: " + metricsResult.getMinEventTimeLatency() + " ms");
        System.out.println();
        System.out.println("---- Processing Time Latency ----");
        System.out.println("Avg: " + metricsResult.getAverageProcessingTimeLatency() + " ms");
        System.out.println("Max: " + metricsResult.getMaxProcessingTimeLatency() + " ms");
        System.out.println("Min: " + metricsResult.getMinProcessingTimeLatency() + " ms");
        System.out.println();
    }
}
