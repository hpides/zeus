package de.hpi.des.mpws2019.benchmark;

import de.hpi.des.mpws2019.benchmark.generator.Generator;
import de.hpi.des.mpws2019.benchmark.generator.UniformGenerator;
import de.hpi.des.mpws2019.engine.Engine;
import de.hpi.des.mpws2019.engine.sink.QueueSink;
import de.hpi.des.mpws2019.engine.source.QueueSource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class Main {

  public static void main(final String[] args) {

    final int eventsPerSecond = 1000000;
    final int maxDelayInSeconds = 5;
    final int timeInSeconds = 30;
    final int numberOfEvents = eventsPerSecond * timeInSeconds;

    final ExecutorService executor =  Executors.newFixedThreadPool(8);

    final TimedBlockingQueue<TupleEvent> timedSource = new TimedBlockingQueue<>(
        maxDelayInSeconds * eventsPerSecond,
        numberOfEvents
    );
    final TimedBlockingQueue<TupleEvent> timedSink = new TimedBlockingQueue<>(
        numberOfEvents);

    final Generator generator = new UniformGenerator(
        eventsPerSecond,
        timeInSeconds,
        executor
    );

    final QueueSource<TupleEvent> queueSource = new QueueSource<>(timedSource);
    final QueueSink<TupleEvent> queueSink = new QueueSink<>(timedSink);

    final Function<TupleEvent, TupleEvent> mapping =
        event -> new TupleEvent(event.getKey(), event.getValue() / 10 + 50);

    final Engine<TupleEvent> engine = new Engine<>(
        queueSource,
        queueSink,
        mapping,
        executor
    );

    final Benchmark benchmark = new Benchmark(generator, engine, timedSource, timedSink);
    BenchmarkResult benchmarkResult = benchmark.run();
    MetricsManager metricsManager = new MetricsManager();
    MetricsResult metricsResult = metricsManager.evaluate(benchmarkResult);
    metricsManager.printMetrics(metricsResult);
  }
}
