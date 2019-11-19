package de.hpi.des.mpws2019.benchmark;

import static java.lang.System.exit;

import de.hpi.des.mpws2019.benchmark.generator.Generator;
import de.hpi.des.mpws2019.engine.Engine;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class Benchmark {
    private final Generator dataGenerator;
    private final Engine engine;
    private final TimedBlockingQueue timedSource;
    private final TimedBlockingQueue timedSink;

    public BenchmarkResult run() {
        log.info("Starting Engine");
        engine.start();
        log.info("Starting Generator");
        final CompletableFuture<Boolean> isFinished = dataGenerator.generate(timedSource);
        BenchmarkResult benchmarkResult = null;

        try {
            isFinished.get();
        } catch (ExecutionException | InterruptedException e) {
            if(e.getCause().getClass().equals(IllegalStateException.class)) {
                log.error("Buffer overflowed, the engine was not able to handle the input EPS. Exiting");
                log.error("Missed Planned Elements: " + (dataGenerator.getTotalEvents()-timedSink.size()));
                exit(-1);
            }
            log.error(e.getMessage());
            e.printStackTrace();
        }
        finally {
            log.info("Waiting for Engine to finish, missing events: " + (dataGenerator.getTotalEvents()-timedSink.size()));
            while(timedSink.size() != dataGenerator.getTotalEvents()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            engine.shutdown();
            benchmarkResult = new BenchmarkResult(dataGenerator, timedSource, timedSink);
        }
        return benchmarkResult;
    }
}
