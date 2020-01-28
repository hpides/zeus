package de.hpi.des.hdes.benchmark;

import static java.lang.System.exit;

import de.hpi.des.hdes.benchmark.generator.Generator;
import de.hpi.des.hdes.engine.Engine;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class Benchmark<IN, OUT> {

  private final Generator<IN> dataGenerator;
  private final Engine engine;

  public void run(BlockingSource<IN> timedSource, BenchmarkingSink<OUT> timedSink) {
    log.info("Starting Engine");
    log.info("Starting Generator");
    final CompletableFuture<Boolean> isFinished = dataGenerator.generate(timedSource);

    try {
      isFinished.get();
      System.out.println(timedSource.getQueue().size());
      System.out.println(timedSink.getTotalCount());
    } catch (ExecutionException | InterruptedException e) {
      if (e.getCause().getClass().equals(IllegalStateException.class)) {
        log.error("Buffer overflowed, the engine was not able to handle the input EPS. Exiting");
        exit(-1);
      }
      log.error(e.getMessage());
      e.printStackTrace();
    } finally {
      log.info("Giving the engine a grace period of a second");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      log.info("Telling Engine to stop processing");
      engine.shutdown();
      dataGenerator.shutdown();
    }
    log.info(Long.toString(timedSink.getTotalCount()));
    log.info(Double.toString(timedSink.getIngestionLatency()));
    log.info(Double.toString(timedSink.getMinLatency()));
    log.info(Double.toString(timedSink.getMaxLatency()));
  }
}
