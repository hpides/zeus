package de.hpi.des.mpws2019.benchmark;

import static org.jooq.lambda.Seq.seq;

import de.hpi.des.mpws2019.benchmark.generator.Generator;
import de.hpi.des.mpws2019.benchmark.generator.UniformGenerator;
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
    private final Generator dataGenerator;
    private final Engine engine;
    private final TimedConcurrentBlockingQueue timedSource;
    private final TimedConcurrentBlockingQueue timedSink;

    public BenchmarkResult run() {
        engine.start();
        final CompletableFuture<Boolean> isFinished = dataGenerator.generate(timedSource);
        BenchmarkResult benchmarkResult = null;

        try {
            isFinished.get();
            benchmarkResult = new BenchmarkResult(dataGenerator, timedSource, timedSink);

        } catch (ExecutionException | InterruptedException e) {
            log.error(e.getMessage());
        } finally {
            engine.shutdown();
        }
        return benchmarkResult;
    }
}
