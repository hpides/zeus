package de.hpi.des.mpws2019.benchmark;

import de.hpi.des.mpws2019.benchmark.generator.Generator;
import de.hpi.des.mpws2019.benchmark.generator.UniformGenerator;
import de.hpi.des.mpws2019.engine.Engine;
import de.hpi.des.mpws2019.engine.graph.TopologyBuilder;
import de.hpi.des.mpws2019.engine.stream.AStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class Main implements Runnable {

  @Option(names = "--eventsPerSecond", defaultValue = "4600000")
  private int eventsPerSecond;
  @Option(names = "--maxDelayInSeconds", defaultValue = "1")
  private int maxDelayInSeconds;
  @Option(names = "--timeInSeconds", defaultValue = "5")
  private int timeInSeconds;
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
    final Benchmark benchmark = new Benchmark(generator, engine);
    Metrics metrics = benchmark.run(source, sink);
    metrics.print();
  }

  public static void main(final String[] args) {
    new CommandLine(new Main()).execute(args);
  }
}
