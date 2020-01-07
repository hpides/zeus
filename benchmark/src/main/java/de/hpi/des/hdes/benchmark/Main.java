package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.benchmark.generator.Generator;
import de.hpi.des.hdes.benchmark.generator.UniformGenerator;
import de.hpi.des.hdes.engine.Engine;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.stream.AStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.log4j.Log4j2;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Log4j2
public class Main implements Runnable {

  @Option(names = {"--eventsPerSecond", "-eps"}, defaultValue = "4600000")
  private int eventsPerSecond;
  @Option(names = {"--maxDelayInSeconds", "-mds"}, defaultValue = "1")
  private int maxDelayInSeconds;
  @Option(names = {"--timeInSeconds", "-tis"}, defaultValue = "5")
  private int timeInSeconds;
  @Option(names = {"--threads", "-t"}, defaultValue = "8")
  private int nThreads;

  @Override
  public void run() {
    long startTime = System.nanoTime();
    log.info("Running with {} EPS, {}s max delay for {}s.",
        eventsPerSecond, maxDelayInSeconds, timeInSeconds);
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

    var engine = new Engine();
    engine.addQuery(builder.build());
    final Benchmark benchmark = new Benchmark(generator, engine);
    Metrics metrics = benchmark.run(source, sink);
    long endTime = System.nanoTime();
    log.info("Finished after {} seconds.", (endTime - startTime) / 1e9);
    metrics.print();
  }

  public static void main(final String[] args) {
    new CommandLine(new Main()).execute(args);
  }
}
