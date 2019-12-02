package de.hpi.des.hdes.benchmark;

import static java.lang.System.exit;
import static java.lang.Thread.sleep;

import de.hpi.des.hdes.benchmark.generator.Generator;
import de.hpi.des.hdes.engine.Engine;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class Benchmark {
    private final Generator dataGenerator;
    private final Engine engine;

    public Metrics run(TimedBlockingSource timedSource, TimedBlockingSink timedSink) {
        log.info("Starting Engine");
        engine.run();
        log.info("Starting Generator");
        final CompletableFuture<Boolean> isFinished = dataGenerator.generate(timedSource);
        Metrics metrics = null;

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

            while(timedSink.size() != dataGenerator.getTotalEvents()) {
                log.info("Waiting for Engine to finish, missing events: " + (dataGenerator.getTotalEvents()-timedSink.size()));
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            log.info("Telling Engine to stop processing");
            engine.shutdown();
            dataGenerator.shutdown();
            metrics = Metrics.from(dataGenerator, timedSource, timedSink);
        }
        return metrics;
    }
}
