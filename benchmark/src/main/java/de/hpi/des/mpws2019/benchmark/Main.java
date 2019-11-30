package de.hpi.des.mpws2019.benchmark;

import de.hpi.des.mpws2019.benchmark.generator.Generator;
import de.hpi.des.mpws2019.benchmark.generator.UniformGenerator;
import de.hpi.des.mpws2019.engine.Engine;
import de.hpi.des.mpws2019.engine.graph.TopologyBuilder;
import de.hpi.des.mpws2019.engine.stream.AStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class Main implements Runnable {

  @Option(names = "--eventsPerSecond", defaultValue = "2000000")
  private int eventsPerSecond = 2_000_000;
  @Option(names = "--maxDelayInSeconds", defaultValue = "1")
  private int maxDelayInSeconds = 1;
  @Option(names = "--timeInSeconds", defaultValue = "5")
  private int timeInSeconds = 5;
  @Option(names = "--threads", defaultValue = "8")
  private int nThreads;

  @Override
  public void run() {
    final ExecutorService executor = Executors.newFixedThreadPool(this.nThreads);
    final Generator generator = new UniformGenerator(
        eventsPerSecond,
        timeInSeconds,
        executor
    );

    final TimedBlockingSource<TupleEvent> source = new TimedBlockingSource<>(
        eventsPerSecond * maxDelayInSeconds);
    final TimedBlockingSink<TupleEvent> sink = new TimedBlockingSink<>();

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

  public static void main(final String[] args) {
    new CommandLine(new Main()).execute(args);
  }
}
