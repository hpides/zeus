package de.hpi.des.mpws2019.benchmark;

import static java.lang.Thread.sleep;

import de.hpi.des.mpws2019.benchmark.generator.Generator;
import de.hpi.des.mpws2019.benchmark.generator.UniformGenerator;
import de.hpi.des.mpws2019.engine.Engine;
import de.hpi.des.mpws2019.engine.graph.TopologyBuilder;
import de.hpi.des.mpws2019.engine.stream.AStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class Main {

  public static void main(final String[] args) {

    final int eventsPerSecond = 2_000_000;
    final int maxDelayInSeconds = 1;
    final int timeInSeconds = 5;

    final ExecutorService executor =  Executors.newFixedThreadPool(8);

    final Generator generator = new UniformGenerator(
            eventsPerSecond,
            timeInSeconds,
            executor
    );

    final TimedBlockingSource<TupleEvent> source = new TimedBlockingSource<>(eventsPerSecond * maxDelayInSeconds);
    final TimedBlockingSink<TupleEvent> sink = new TimedBlockingSink<>();

    final Function<TupleEvent, TupleEvent> mapping =
        event -> new TupleEvent(event.getKey(), event.getValue() / 10 + 50, event.isBenchmarkCheckpoint());

    final TopologyBuilder builder = new TopologyBuilder();
    final AStream<TupleEvent> stream = builder.streamOf(source)
            .map(e -> new TupleEvent(e.getKey(), e.getValue() + 1, e.isBenchmarkCheckpoint()));
    stream.to(sink);



    var engine = new Engine(builder);
    final Benchmark benchmark = new Benchmark(generator, engine, source, sink);
    BenchmarkResult benchmarkResult = benchmark.run();
    MetricsManager metricsManager = new MetricsManager();
    MetricsResult metricsResult = metricsManager.evaluate(benchmarkResult);
    metricsManager.printMetrics(metricsResult);
  }
}
